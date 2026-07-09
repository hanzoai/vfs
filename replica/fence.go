// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package replica

// Fencing turns the unconditional Store.Put — whose object-store contract is
// literally "overwriting is allowed" — into a round-fenced write, so a deposed or
// partitioned writer can no longer clobber a newer owner's snapshot. It is the
// STORAGE half of single-writer safety: coordination (github.com/hanzoai/ha)
// decides who writes and at what monotone round; FencedStore makes the object
// store REJECT a write stamped with a superseded round, no matter what a stale
// local view believes.
//
// # The admission rule
//
// One object per key carries a small round header in front of the payload. The
// store — never this process — enforces, atomically:
//
//	admit a Put at `round` iff round >= the round recorded in the object;
//	reject with ErrStaleRound when round < recorded.
//
// `>=` (not the one-shot fence's strict `>`) is deliberate and is the whole
// difference from an epoch-per-write fence: the single owner re-ships the SAME
// object many times at its STABLE lease round (every PushLoop tick), so
// same-round overwrites by the current owner MUST be admitted; only a LOWER
// round — the signature of a writer whose lease was taken over and whose
// successor already advanced the recorded round — is refused. A new owner seals
// the object to its higher round exactly once (a claim Put at recorded+1); from
// that instant every lower-round ship by the old owner is fenced forever.
//
// # No admit/write window
//
// The round header and the payload live in ONE object and advance in ONE
// conditional write. There is therefore no interval between "round admitted" and
// "data written" for a second writer to slip into — the exact window a two-object
// design (separate lease record + data object) would have to close by other
// means. Because a takeover's claim Put is a compare-and-set read-modify-write
// conditioned on the version just read, an owner that slips a write in between the
// successor's read and its claim only makes the claim retry (re-reading the newer
// snapshot), so the successor always carries forward the latest durably-shipped
// bytes before it seals — no acknowledged data is lost.
//
// FencedStore composes over the ConditionalStore seam (S3 If-Match /
// GCS generation-match) and stays dependency-free; the concrete CAS store lives
// with whoever owns an S3/SeaweedFS client.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	// ErrNotFound is returned by ConditionalStore.Get for a key never written.
	// FencedStore treats an absent object as recorded round 0, so any real lease
	// round (>= 1) is admissible against an empty slot.
	ErrNotFound = errors.New("replica: fenced object not found")

	// ErrStaleRound is returned when a Put's round is below the round already
	// recorded for the object — the deposed/partitioned-writer rejection. The
	// object is left byte-for-byte untouched. This is the invariant that closes
	// the split-brain double-write: once a successor advances the recorded round,
	// the predecessor can never write again.
	ErrStaleRound = errors.New("replica: write round is below the recorded round (writer fenced)")

	// ErrConflict is returned when the atomic conditional write lost a race to a
	// concurrent writer (the object changed between our read and our write).
	// Put retries internally; ErrConflict escaping Put means the bounded retries
	// were exhausted under sustained contention, not that a caller should blindly
	// retry with the same round.
	ErrConflict = errors.New("replica: conditional write lost a concurrent race")
)

// maxCASAttempts bounds Put's read-check-CAS retry loop so a pathologically hot
// object fails loudly rather than spinning forever. One retry suffices for any
// single genuine race; further attempts only absorb repeated contention.
const maxCASAttempts = 8

// ConditionalStore is the atomic compare-and-set object surface the fence needs:
// the S3 If-Match / GCS generation-match primitive, nothing more. PutIfVersion
// MUST evaluate its precondition atomically, server-side; a client-side
// "Get, compare in Go, then Put" is NOT a valid implementation — that
// non-atomicity is the exact race this fence exists to close. The concrete
// implementation lives with the caller that owns an S3/SeaweedFS client (e.g.
// cloud's minio-go If-Match store); this package stays dependency-free over the
// seam, and the in-memory fake in fence_test.go models the same server-side CAS.
type ConditionalStore interface {
	// Get returns the object bytes and the store's authoritative version token
	// (ETag/generation) for key, or a wrapped ErrNotFound when key is absent.
	Get(ctx context.Context, key string) (data []byte, version string, err error)
	// PutIfVersion writes data at key iff the store's CURRENT version for key
	// still equals expectVersion ("" means the object must not exist yet — a
	// create-only put, i.e. If-None-Match: *) and returns the new version. A
	// failed precondition is reported as a wrapped ErrConflict.
	PutIfVersion(ctx context.Context, key string, data []byte, expectVersion string) (newVersion string, err error)
}

// FencedStore is a round-fenced writer over a ConditionalStore: one object per
// key, a monotone round admitted only when at least the recorded one.
type FencedStore struct{ store ConditionalStore }

// NewFencedStore builds a FencedStore over an atomic-CAS store.
func NewFencedStore(store ConditionalStore) *FencedStore { return &FencedStore{store: store} }

// fenceMagic prefixes every record so a foreign object at the same key is
// detected (errCorrupt) rather than silently misparsed as an absurd round.
const fenceMagic = "rfnc1\x00"

func encode(round uint64, payload []byte) []byte {
	buf := make([]byte, 0, len(fenceMagic)+8+len(payload))
	buf = append(buf, fenceMagic...)
	var r [8]byte
	binary.BigEndian.PutUint64(r[:], round)
	buf = append(buf, r[:]...)
	return append(buf, payload...)
}

// errCorrupt is returned when a stored object does not decode as a record this
// package wrote — fail closed rather than invent a round from arbitrary bytes.
var errCorrupt = errors.New("replica: stored object is not a valid fenced record")

func decode(b []byte) (round uint64, payload []byte, err error) {
	m := len(fenceMagic)
	if len(b) < m+8 || string(b[:m]) != fenceMagic {
		return 0, nil, errCorrupt
	}
	return binary.BigEndian.Uint64(b[m : m+8]), b[m+8:], nil
}

// Put admits and (atomically with admission) writes payload for key at round. It
// rejects with ErrStaleRound when round is below the recorded round — the fence —
// before any write is attempted, then advances the header and appends the payload
// in a single PutIfVersion conditioned on the version read THIS attempt (never a
// cached one). If a concurrent writer's write lands first, our CAS fails
// (ErrConflict) and we retry: re-read the now-authoritative round, re-run the
// strict check (a lower-round racer now loses as ErrStaleRound; an equal-or-higher
// one retries the CAS against the new version and, absent further contention,
// wins). The store is the sole arbiter of "what round currently owns this key."
func (f *FencedStore) Put(ctx context.Context, key string, payload []byte, round uint64) error {
	if f == nil || f.store == nil {
		return fmt.Errorf("replica: nil fenced store")
	}
	if key == "" {
		return fmt.Errorf("replica: fenced put requires a key")
	}
	var lastErr error
	for attempt := 0; attempt < maxCASAttempts; attempt++ {
		recorded, version, err := f.current(ctx, key)
		if err != nil {
			return err
		}
		if round < recorded {
			return fmt.Errorf("%w: key=%s round=%d recorded=%d", ErrStaleRound, key, round, recorded)
		}
		if _, err := f.store.PutIfVersion(ctx, key, encode(round, payload), version); err != nil {
			if errors.Is(err, ErrConflict) {
				lastErr = err
				continue // re-read the winner's state and retry.
			}
			return fmt.Errorf("replica: fenced put %s: %w", key, err)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = ErrConflict
	}
	return fmt.Errorf("%w: exhausted %d attempts fencing %s at round %d", lastErr, maxCASAttempts, key, round)
}

// CarryForward is the safe TAKEOVER seal: it reads the latest durably-stored
// payload for key and re-seals it UNCHANGED at round, invoking hydrate on that
// payload first. It exists because a successor must never seal a snapshot it read
// BEFORE a predecessor's last landed ship, or that acknowledged write would be
// clobbered. The read and the seal are one compare-and-set on the same version:
// if a predecessor write lands in between, the CAS conflicts and CarryForward
// retries — re-reading the newer payload, re-hydrating, and re-sealing IT — so the
// successor always advances past exactly the state it carried forward, never a
// stale one. After CarryForward returns, every predecessor write at a round below
// `round` is fenced.
//
// round is the successor's lease round (issued monotone by the Fencer); it must be
// >= the recorded round or CarryForward returns ErrStaleRound (a newer owner
// already passed us — we lost the race and must step aside). hydrate is the
// caller's restore-into-local-DB step; it may run more than once across retries,
// so it must be idempotent (a whole-snapshot restore is).
func (f *FencedStore) CarryForward(ctx context.Context, key string, round uint64, hydrate func(payload []byte) error) error {
	if f == nil || f.store == nil {
		return fmt.Errorf("replica: nil fenced store")
	}
	if key == "" {
		return fmt.Errorf("replica: carry-forward requires a key")
	}
	var lastErr error
	for attempt := 0; attempt < maxCASAttempts; attempt++ {
		data, version, err := f.store.Get(ctx, key)
		var recorded uint64
		var payload []byte
		switch {
		case errors.Is(err, ErrNotFound):
			recorded, payload, version = 0, nil, ""
		case err != nil:
			return fmt.Errorf("replica: carry-forward read %s: %w", key, err)
		default:
			if recorded, payload, err = decode(data); err != nil {
				return err
			}
		}
		if round < recorded {
			return fmt.Errorf("%w: key=%s round=%d recorded=%d", ErrStaleRound, key, round, recorded)
		}
		if hydrate != nil {
			if err := hydrate(payload); err != nil {
				return fmt.Errorf("replica: carry-forward hydrate %s: %w", key, err)
			}
		}
		if _, err := f.store.PutIfVersion(ctx, key, encode(round, payload), version); err != nil {
			if errors.Is(err, ErrConflict) {
				lastErr = err
				continue // a predecessor's write landed; re-read it and carry it forward.
			}
			return fmt.Errorf("replica: carry-forward seal %s: %w", key, err)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = ErrConflict
	}
	return fmt.Errorf("%w: exhausted %d attempts carrying forward %s at round %d", lastErr, maxCASAttempts, key, round)
}

// Get returns the fenced payload and the round it was written at, or ErrNotFound
// when key is absent. A reader restores the payload; a successor reads the round
// to learn what to advance past on takeover.
func (f *FencedStore) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	data, _, err := f.store.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	round, payload, err := decode(data)
	if err != nil {
		return nil, 0, err
	}
	return payload, round, nil
}

// Round returns the round currently recorded for key, or 0 if the object is
// absent — the value a new owner increments by one to claim the writer role.
func (f *FencedStore) Round(ctx context.Context, key string) (uint64, error) {
	round, _, err := f.current(ctx, key)
	return round, err
}

// current returns the recorded round (0 if absent) and the store version token to
// condition the next write on.
func (f *FencedStore) current(ctx context.Context, key string) (round uint64, version string, err error) {
	data, version, err := f.store.Get(ctx, key)
	switch {
	case errors.Is(err, ErrNotFound):
		return 0, "", nil
	case err != nil:
		return 0, "", fmt.Errorf("replica: read fenced object %s: %w", key, err)
	}
	round, _, err = decode(data)
	if err != nil {
		return 0, "", err
	}
	return round, version, nil
}
