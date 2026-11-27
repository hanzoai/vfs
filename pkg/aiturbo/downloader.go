// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package aiturbo

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/hanzoai/vfs/internal/blake3"
	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/hanzoai/vfs/pkg/fence"
)

// RangeSpan coalesces one or more nearby item extents inside a single immutable pack
// into one contiguous GetRange request.
type RangeSpan struct {
	PackKey string
	Start   int64
	End     int64
	Items   []EntryRef
}

// GroupedDownloader retrieves checkpoint manifests and coalesces nearby extent reads
// into bounded GetRange operations. When Options.GroupSize == 1, coalescing is disabled
// cleanly to provide a matched parallel ungrouped baseline over the exact same code path.
type GroupedDownloader struct {
	be      backend.Backend
	counter *CountingBackend
	ownCnt  bool
	fence   *fence.Store
	opt     Options
}

// NewGroupedDownloader initializes a grouped downloader over be.
func NewGroupedDownloader(be backend.Backend, opt Options) (*GroupedDownloader, error) {
	if be == nil {
		return nil, errors.New("aiturbo: nil backend")
	}
	opt = opt.withDefaults()
	cnt, ok := be.(*CountingBackend)
	own := false
	if !ok {
		cnt = NewCountingBackend(be, opt.SimRTT)
		own = true
	}
	return &GroupedDownloader{
		be:      cnt,
		counter: cnt,
		ownCnt:  own,
		fence:   fence.New(cnt, maxHeadSize),
		opt:     opt,
	}, nil
}

// Counter returns the underlying wire counter.
func (d *GroupedDownloader) Counter() *CountingBackend {
	return d.counter
}

// ReadHeadManifest reads the current fenced "head" pointer and loads the referenced
// immutable checkpoint Manifest.
func (d *GroupedDownloader) ReadHeadManifest(ctx context.Context) (Manifest, uint64, error) {
	manKeyBytes, round, err := d.fence.Get(ctx, headKey)
	if err != nil {
		return Manifest{}, 0, fmt.Errorf("aiturbo: read head: %w", err)
	}
	raw, _, err := d.be.Get(ctx, string(manKeyBytes), maxManifestBytes)
	if err != nil {
		return Manifest{}, 0, fmt.Errorf("aiturbo: read manifest %q: %w", string(manKeyBytes), err)
	}
	m, err := DecodeManifest(raw)
	if err != nil {
		return Manifest{}, 0, err
	}
	return m, round, nil
}

// PlanRangeReads groups EntryRefs by pack key and coalesces spans within
// CoalesceGapBytes up to MaxCoalescedRange (or 1-by-1 when GroupSize == 1).
func (d *GroupedDownloader) PlanRangeReads(refs []EntryRef) []RangeSpan {
	if len(refs) == 0 {
		return nil
	}
	byPack := make(map[string][]EntryRef)
	for _, r := range refs {
		byPack[r.PackKey] = append(byPack[r.PackKey], r)
	}
	var packKeys []string
	for k := range byPack {
		packKeys = append(packKeys, k)
	}
	sort.Strings(packKeys)

	var spans []RangeSpan
	for _, pk := range packKeys {
		list := byPack[pk]
		sort.Slice(list, func(i, j int) bool { return list[i].Offset < list[j].Offset })

		if d.opt.GroupSize == 1 {
			for _, e := range list {
				spans = append(spans, RangeSpan{
					PackKey: pk,
					Start:   int64(e.Offset),
					End:     int64(e.Offset + e.Length),
					Items:   []EntryRef{e},
				})
			}
			continue
		}

		cur := RangeSpan{
			PackKey: pk,
			Start:   int64(list[0].Offset),
			End:     int64(list[0].Offset + list[0].Length),
			Items:   []EntryRef{list[0]},
		}
		for _, e := range list[1:] {
			eStart := int64(e.Offset)
			eEnd := int64(e.Offset + e.Length)
			gap := max(int64(0), eStart-cur.End)
			newEnd := max(cur.End, eEnd)
			if len(cur.Items) < d.opt.GroupSize &&
				gap <= d.opt.CoalesceGapBytes &&
				newEnd-cur.Start <= d.opt.MaxCoalescedRange {
				cur.End = newEnd
				cur.Items = append(cur.Items, e)
			} else {
				spans = append(spans, cur)
				cur = RangeSpan{
					PackKey: pk,
					Start:   eStart,
					End:     eEnd,
					Items:   []EntryRef{e},
				}
			}
		}
		spans = append(spans, cur)
	}
	return spans
}

// DownloadCheckpoint restores selected keys (or all keys in manifest if wanted is empty)
// using coalesced GetRange requests, verifying every slice's BLAKE3 digest.
func (d *GroupedDownloader) DownloadCheckpoint(ctx context.Context, wanted []string) (map[string][]byte, *Metrics, error) {
	if d.ownCnt {
		d.counter.Reset()
	}
	start := time.Now()
	m, _, err := d.ReadHeadManifest(ctx)
	if err != nil {
		return nil, nil, err
	}

	var refs []EntryRef
	if len(wanted) == 0 {
		refs = m.Entries
	} else {
		wantSet := make(map[string]struct{}, len(wanted))
		for _, w := range wanted {
			wantSet[w] = struct{}{}
		}
		for _, e := range m.Entries {
			if _, ok := wantSet[e.ItemKey]; ok {
				refs = append(refs, e)
				delete(wantSet, e.ItemKey)
			}
		}
		if len(wantSet) > 0 {
			return nil, nil, fmt.Errorf("%w: missing %d requested checkpoint item(s)", backend.ErrNotFound, len(wantSet))
		}
	}

	res, err := d.fetchSpans(ctx, d.PlanRangeReads(refs))
	if err != nil {
		return nil, nil, err
	}

	var totalBytes int64
	for _, b := range res {
		totalBytes += int64(len(b))
	}
	dur := time.Since(start)
	if dur <= 0 {
		dur = time.Nanosecond
	}
	return res, &Metrics{
		TotalItemBytes: totalBytes,
		TotalObjects:   len(res),
		PlannedPacks:   len(refs),
		Duration:       dur,
		BytesPerSec:    float64(totalBytes) / dur.Seconds(),
		Wire:           d.counter.Snapshot(),
	}, nil
}

func (d *GroupedDownloader) fetchSpans(ctx context.Context, spans []RangeSpan) (map[string][]byte, error) {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(map[string][]byte)
	var outMu sync.Mutex
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	sem := make(chan struct{}, d.opt.Streams)
	var wg sync.WaitGroup

	for _, sp := range spans {
		select {
		case sem <- struct{}{}:
		case <-workCtx.Done():
			setErr(workCtx.Err())
			goto waitDone
		}

		wg.Add(1)
		go func(span RangeSpan) {
			defer wg.Done()
			defer func() { <-sem }()

			buf, err := d.be.GetRange(workCtx, span.PackKey, span.Start, span.End-span.Start)
			if err != nil {
				setErr(fmt.Errorf("aiturbo: getrange %q [%d,%d): %w", span.PackKey, span.Start, span.End, err))
				return
			}
			local := make(map[string][]byte, len(span.Items))
			for _, it := range span.Items {
				rel := int64(it.Offset) - span.Start
				end := rel + int64(it.Length)
				if rel < 0 || end > int64(len(buf)) {
					setErr(fmt.Errorf("%w: range out of bounds for %q", ErrCorruptObject, it.ItemKey))
					return
				}
				slice := append([]byte(nil), buf[rel:end]...)
				if blake3.Sum(slice) != it.Digest {
					setErr(fmt.Errorf("%w: digest mismatch for %q", ErrCorruptObject, it.ItemKey))
					return
				}
				local[it.ItemKey] = slice
			}
			outMu.Lock()
			for k, v := range local {
				out[k] = v
			}
			outMu.Unlock()
		}(sp)
	}

waitDone:
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
