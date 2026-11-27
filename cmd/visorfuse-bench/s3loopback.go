// Copyright 2026 Hanzo AI, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// loopbackS3 is an in-process HTTP S3 protocol server so the benchmark drives
// the real pkg/backend/s3 Hanzo S3 SDK over HTTP with full conditional-write
// (If-None-Match / If-Match) semantics and zero external service dependencies.
type loopbackS3 struct {
	bucket string
	ln     net.Listener
	srv    *http.Server

	mu   sync.Mutex
	objs map[string][]byte
	mod  map[string]time.Time
}

func startLoopbackS3(bucket string) (*loopbackS3, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &loopbackS3{
		bucket: bucket,
		ln:     ln,
		objs:   make(map[string][]byte),
		mod:    make(map[string]time.Time),
	}
	s.srv = &http.Server{Handler: http.HandlerFunc(s.serve)}
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

func (s *loopbackS3) Endpoint() string { return s.ln.Addr().String() }

func (s *loopbackS3) Close() error {
	return s.srv.Close()
}

func (s *loopbackS3) fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func s3ETag(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func decodeAWSBody(r *http.Request) ([]byte, error) {
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") &&
		!strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return io.ReadAll(r.Body)
	}
	br := bufio.NewReader(r.Body)
	var out []byte
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		n, err := strconv.ParseInt(size, 16, 64)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
		chunk := make([]byte, n)
		if _, err := io.ReadFull(br, chunk); err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		if _, err := br.Discard(2); err != nil {
			return nil, err
		}
	}
}

func matchETagHeader(header, etag string) bool {
	for _, e := range strings.Split(header, ",") {
		if e = strings.TrimSpace(e); e == "*" || e == etag {
			return true
		}
	}
	return false
}

func (s *loopbackS3) serve(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if bucket != s.bucket {
		s.fail(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	q := r.URL.Query()
	if key == "" {
		switch {
		case q.Has("location"):
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
		case q.Get("list-type") == "2":
			s.serveList(w, q.Get("prefix"))
		default:
			s.fail(w, http.StatusBadRequest, "InvalidRequest")
		}
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cur, exists := s.objs[key]
	switch r.Method {
	case http.MethodPut:
		data, err := decodeAWSBody(r)
		if err != nil {
			s.fail(w, http.StatusBadRequest, "IncompleteBody")
			return
		}
		if h := r.Header.Get("If-None-Match"); h != "" && exists && matchETagHeader(h, s3ETag(cur)) {
			s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		if h := r.Header.Get("If-Match"); h != "" && (!exists || !matchETagHeader(h, s3ETag(cur))) {
			s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		s.objs[key] = data
		s.mod[key] = time.Now()
		w.Header().Set("ETag", s3ETag(data))
	case http.MethodGet, http.MethodHead:
		if !exists {
			s.fail(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", s3ETag(cur))
		w.Header().Set("Last-Modified", s.mod[key].UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")
		data, status := cur, http.StatusOK
		if rg := r.Header.Get("Range"); rg != "" {
			var a, b int
			if _, err := fmt.Sscanf(rg, "bytes=%d-%d", &a, &b); err != nil || a > b || a >= len(cur) {
				s.fail(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
				return
			}
			b = min(b, len(cur)-1)
			data, status = cur[a:b+1], http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(cur)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(status)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case http.MethodDelete:
		delete(s.objs, key)
		delete(s.mod, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		s.fail(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func (s *loopbackS3) serveList(w http.ResponseWriter, prefix string) {
	s.mu.Lock()
	var keys []string
	for k := range s.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sizes := make(map[string]int, len(keys))
	mods := make(map[string]time.Time, len(keys))
	for _, k := range keys {
		sizes[k] = len(s.objs[k])
		mods[k] = s.mod[k]
	}
	s.mu.Unlock()

	sort.Strings(keys)
	var out bytes.Buffer
	out.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
	fmt.Fprintf(&out, "<KeyCount>%d</KeyCount>", len(keys))
	for _, k := range keys {
		out.WriteString("<Contents><Key>")
		_ = xml.EscapeText(&out, []byte(k))
		fmt.Fprintf(&out, "</Key><LastModified>%s</LastModified><Size>%d</Size></Contents>",
			mods[k].UTC().Format(time.RFC3339), sizes[k])
	}
	out.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(out.Bytes())
}
