// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/internal/storetest"
	"github.com/hanzoai/vfs/pkg/aiturbo"
	"github.com/hanzoai/vfs/pkg/backend"
	s3be "github.com/hanzoai/vfs/pkg/backend/s3"
)

// TestAITURBOVolumeAndRealS3Integration exercises AITURBO grouped checkpoint
// publication and restoration against both storetest.S3 and a real hanzoai/s3
// process (when `s3` is on PATH), alongside canonical vfs.Volume objects.
func TestAITURBOVolumeAndRealS3Integration(t *testing.T) {
	runPair := func(t *testing.T, name string, newBE func(t *testing.T, prefix string) backend.Backend) {
		t.Run(name, func(t *testing.T) {
			// 1. Canonical vfs.Volume checkpoint write & flush through CountingBackend
			beVol := newBE(t, "vol")
			cntVol := aiturbo.NewCountingBackend(beVol, 0)
			cipher := vfsAge(t)
			if err := vfs.Create(ctx, cfg(cntVol, cipher, 0)); err != nil {
				t.Fatalf("vfs.Create: %v", err)
			}
			v, err := vfs.Attach(ctx, cfg(cntVol, cipher, 1))
			if err != nil {
				t.Fatalf("vfs.Attach: %v", err)
			}
			defer v.Close()

			const numFiles = 32
			wantFiles := make(map[string][]byte, numFiles)
			for i := range numFiles {
				nm := fmt.Sprintf("f-%03d-bin", i)
				data := content(uint64(900+i), 8192)
				wantFiles[nm] = data
				write(t, v, vfs.RootInode, nm, data)
			}
			flush(t, v)
			volWire := cntVol.Snapshot()

			// Mirror canonical Volume file payloads through AITURBO vs matched parallel baseline (Streams=8)
			items := make([]aiturbo.Item, 0, numFiles)
			for nm, data := range wantFiles {
				items = append(items, aiturbo.Item{
					Key:  "data/" + nm,
					Data: data,
				})
			}

			beBase := aiturbo.NewCountingBackend(newBE(t, "base"), 0)
			upBase, _ := aiturbo.NewGroupedUploader(beBase, aiturbo.Options{GroupSize: 1, Streams: 8})
			planBase, err := upBase.Planner().PlanCheckpoint(1, items)
			if err != nil {
				t.Fatal(err)
			}
			mBase, err := upBase.UploadAndCommit(ctx, planBase, 1)
			if err != nil {
				t.Fatal(err)
			}

			beTurbo := aiturbo.NewCountingBackend(newBE(t, "turbo"), 0)
			upTurbo, _ := aiturbo.NewGroupedUploader(beTurbo, aiturbo.Options{GroupSize: 8, Streams: 8})
			planTurbo, err := upTurbo.Planner().PlanCheckpoint(1, items)
			if err != nil {
				t.Fatal(err)
			}
			mTurbo, err := upTurbo.UploadAndCommit(ctx, planTurbo, 1)
			if err != nil {
				t.Fatal(err)
			}

			downTurbo, _ := aiturbo.NewGroupedDownloader(beTurbo, aiturbo.Options{GroupSize: 8, Streams: 8})
			beTurbo.Reset()
			got, dmTurbo, err := downTurbo.DownloadCheckpoint(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}

			for nm, data := range wantFiles {
				if !bytes.Equal(got["data/"+nm], data) {
					t.Fatalf("restored mismatch for %s", nm)
				}
			}

			if mTurbo.Wire.PutIfAbsents >= mBase.Wire.PutIfAbsents {
				t.Fatalf("%s: expected fewer PUTs for turbo (%d) than parallel baseline (%d)",
					name, mTurbo.Wire.PutIfAbsents, mBase.Wire.PutIfAbsents)
			}

			t.Logf("[%s] canonical vfs.Volume flushed: puts=%d bytes=%d | matched baseline PUTs=%d | AITURBO PUTs=%d | AITURBO Download GetRanges=%d",
				name, volWire.TotalWriteRequests(), volWire.BytesUploaded, mBase.Wire.PutIfAbsents, mTurbo.Wire.PutIfAbsents, dmTurbo.Wire.GetRanges)
		})
	}

	runPair(t, "storetest-s3", func(t *testing.T, prefix string) backend.Backend {
		srv := storetest.NewS3(t, "disk")
		be, err := s3be.New(s3be.Config{Endpoint: srv.Endpoint(), Insecure: true, Bucket: "disk", Prefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = be.Close() })
		return be
	})

	if storetest.HaveReal() {
		runPair(t, "real-hanzo-s3", func(t *testing.T, prefix string) backend.Backend {
			r := storetest.RealS3(t)
			be, err := s3be.New(s3be.Config{
				Endpoint:  r.Endpoint,
				Insecure:  true,
				Bucket:    r.Bucket,
				Prefix:    r.Prefix("aiturbo-" + prefix),
				AccessKey: r.AccessKey,
				SecretKey: r.SecretKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = be.Close() })
			return be
		})
	}
}
