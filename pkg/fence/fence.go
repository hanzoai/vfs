// Copyright 2025 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package fence turns the unconditional Put of an object store, whose contract
// is literally "overwriting is allowed", into a round-fenced write, so a deposed
// or partitioned writer can no longer clobber a newer owner's object. A Volume's
// Head and a replicated SQLite snapshot are both fenced here: this is the one
// fencing implementation.
//
// It is the STORAGE half of single-writer safety: coordination decides who
// writes and at what monotone round; Store makes the object store REJECT a write
// stamped with a superseded round, no matter what a stale local view believes.
//
// # The admission rule
//
// One object per key carries a small round header in front of the payload. The
// store, never this process, enforces atomically:
//
//	admit a Put at `round` iff round >= the round recorded in the object;
//	reject with ErrStaleRound when round < recorded.
//
// `>=` (not the one-shot fence's strict `>`) is deliberate: the single owner
// re-ships the SAME object many times at its STABLE round, so same-round
// overwrites by the current owner MUST be admitted; only a LOWER round, the
// signature of a writer whose lease was taken over and whose successor already
// advanced the recorded round, is refused. A new owner seals the object to its
// higher round exactly once (Claim); from that instant every lower-round ship by
// the old owner is fenced forever.
//
// # No admit/write window
//
// The round header and the payload live in ONE object and advance in ONE
// conditional write. There is therefore no interval between "round admitted" and
// "data written" for a second writer to slip into. Because a takeover's claim is
// a compare-and-set conditioned on the version just read, an owner that slips a
// write in between the successor's read and its claim only makes the claim retry
// (re-reading the newer payload), so the successor always carries forward the
// latest durably-shipped bytes before it seals: no acknowledged data is lost.
package fence

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
)

var (
	// ErrStaleRound is returned when the round does not hold the object: a Put at a
	// round below the one already recorded, or a Claim at a round that is not above
	// it. It is the deposed or partitioned writer rejection. The object is left
	// byte-for-byte untouched. Once a successor advances the recorded round, the
	// predecessor can never write again.
	ErrStaleRound = errors.New("fence: write round is behind the recorded round (writer fenced)")

	// errCorrupt is returned when a stored object does not decode as a record this
	// package wrote: fail closed rather than invent a round from arbitrary bytes.
	errCorrupt = errors.New("fence: stored object is not a valid fenced record")
)

// maxCASAttempts bounds the read-check-CAS retry loops so a pathologically hot
// object fails loudly rather than spinning forever. One retry suffices for any
// single genuine race; further attempts only absorb repeated contention.
const maxCASAttempts = 8

// Backend is the part of backend.Backend the fence needs: a versioned read and
// the two atomic conditional writes. PutIfAbsent and PutIfVersion MUST evaluate
// their precondition atomically, server-side; a client-side "Get, compare in Go,
// then Put" is NOT a valid implementation, since that gap is the exact race this
// fence exists to close.
type Backend interface {
	Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error)
	PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error)
	PutIfVersion(ctx context.Context, key string, data []byte, v backend.Version) (backend.Version, error)
}

// Store is a round-fenced writer over a Backend: one object per key, a monotone
// round admitted only when at least the recorded one. Every write it makes is
// stamped with a token no other Store shares, so a Store that finds its own write
// already in place (its first attempt landed and the reply was lost) knows it
// succeeded rather than lost.
type Store struct {
	be    Backend
	max   int64
	token [tokenLen]byte

	mu   sync.Mutex
	last map[string]landed // what each key was after this Store's last write that landed
}

// landed is what a key was left as by a write of this Store: its version, and the round
// the write was made at.
type landed struct {
	version backend.Version
	round   uint64
}

const tokenLen = 16

// New builds a Store over a conditional-write backend. max bounds the size of the
// objects it will read and the size of those it will write (header and payload): a
// record that a reader of this Store would refuse is never written, so a commit that
// would brick the object fails instead, with ErrTooLarge and nothing changed.
func New(be Backend, max int64) *Store {
	s := &Store{be: be, max: max, last: map[string]landed{}}
	if _, err := rand.Read(s.token[:]); err != nil {
		panic("fence: no randomness: " + err.Error())
	}
	return s
}

// magic prefixes every record so a foreign object at the same key is detected
// (errCorrupt) rather than silently misparsed as an absurd round.
const magic = "fence\x00"

func (s *Store) encode(round uint64, payload []byte) []byte {
	buf := make([]byte, 0, len(magic)+8+tokenLen+len(payload))
	buf = append(buf, magic...)
	buf = binary.BigEndian.AppendUint64(buf, round)
	buf = append(buf, s.token[:]...)
	return append(buf, payload...)
}

func decode(b []byte) (round uint64, payload []byte, err error) {
	m := len(magic)
	if len(b) < m+8+tokenLen || string(b[:m]) != magic {
		return 0, nil, errCorrupt
	}
	return binary.BigEndian.Uint64(b[m : m+8]), b[m+8+tokenLen:], nil
}

// fits refuses a record that the Store itself would not read back.
func (s *Store) fits(record []byte) error {
	if int64(len(record)) > s.max {
		return fmt.Errorf("fence: a record of %d bytes is over the bound of %d that every reader holds: %w", len(record), s.max, backend.ErrTooLarge)
	}
	return nil
}

// write conditions a write on the version just read: create-only when the key was
// absent (version ""), else compare-and-set on that version.
func (s *Store) write(ctx context.Context, key string, data []byte, v backend.Version, round uint64) error {
	var (
		now backend.Version
		err error
	)
	if v == "" {
		now, err = s.be.PutIfAbsent(ctx, key, data)
	} else {
		now, err = s.be.PutIfVersion(ctx, key, data, v)
	}
	switch {
	case err == nil:
		s.remember(key, now, round)
	case !errors.Is(err, backend.ErrConflict):
		s.forget(key) // the write may have landed: what the object is now is not known
	}
	return err
}

// remember keeps the version a write of this Store left behind: the next Put of the key
// is conditioned on it and needs no read first.
func (s *Store) remember(key string, v backend.Version, round uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last[key] = landed{v, round}
}

func (s *Store) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.last, key)
}

// known returns the version this Store left the key at, when it left it at round: a Put
// at another round is not the write the version was kept for.
func (s *Store) known(key string, round uint64) (backend.Version, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.last[key]
	return l.version, ok && l.version != "" && l.round == round
}

// read returns the object's bytes and version, or nil and "" if it is absent.
func (s *Store) read(ctx context.Context, key string) ([]byte, backend.Version, error) {
	data, version, err := s.be.Get(ctx, key, s.max)
	if errors.Is(err, backend.ErrNotFound) {
		return nil, "", nil
	}
	return data, version, err
}

// Create writes payload at key at round 0, only if key does not exist. It is how
// an object is born: Put would admit round 0 over an existing round-0 object.
// If key exists the error wraps backend.ErrConflict.
func (s *Store) Create(ctx context.Context, key string, payload []byte) error {
	record := s.encode(0, payload)
	if err := s.fits(record); err != nil {
		return fmt.Errorf("fence: create %s: %w", key, err)
	}
	v, err := s.be.PutIfAbsent(ctx, key, record)
	if err != nil {
		return fmt.Errorf("fence: create %s: %w", key, err)
	}
	s.remember(key, v, 0)
	return nil
}

// Put admits and (atomically with admission) writes payload for key at round. It
// rejects with ErrStaleRound when round is below the recorded round, the fence,
// before any write is attempted, then advances the header and replaces the
// payload in a single conditional write on the version read THIS attempt (never a
// cached one). If a concurrent writer's write lands first, our CAS fails and we
// retry: re-read the now-authoritative round, re-run the check (a lower-round
// racer now loses as ErrStaleRound; an equal-or-higher one retries the CAS
// against the new version). If the re-read finds exactly the bytes we wrote, an
// earlier attempt of ours landed and Put reports success. The store is the sole
// arbiter of which round owns the key. An absent key is recorded round 0, so any
// round admits a first write.
func (s *Store) Put(ctx context.Context, key string, payload []byte, round uint64) error {
	if s == nil || s.be == nil {
		return fmt.Errorf("fence: nil store")
	}
	want := s.encode(round, payload)
	if err := s.fits(want); err != nil {
		return fmt.Errorf("fence: put %s: %w", key, err)
	}
	// The steady state is one request: this Store's last write left the object at a
	// version it kept, and a write conditioned on it is admitted by the store exactly
	// when nothing has changed the object since, which is when the recorded round is
	// still the one this writer holds. A claim by a higher round changes the version, so
	// the write loses and the loop below reads the round that now holds the key.
	if v, ok := s.known(key, round); ok {
		err := s.write(ctx, key, want, v, round)
		if err == nil {
			return nil
		}
		if !errors.Is(err, backend.ErrConflict) {
			return fmt.Errorf("fence: put %s: %w", key, err)
		}
		s.forget(key)
	}
	var lastErr error
	for range maxCASAttempts {
		data, version, err := s.read(ctx, key)
		if err != nil {
			return fmt.Errorf("fence: read %s: %w", key, err)
		}
		if bytes.Equal(data, want) {
			return nil
		}
		var recorded uint64
		if data != nil {
			if recorded, _, err = decode(data); err != nil {
				return err
			}
		}
		if round < recorded {
			return fmt.Errorf("%w: key=%s round=%d recorded=%d", ErrStaleRound, key, round, recorded)
		}
		if err := s.write(ctx, key, want, version, round); err != nil {
			if errors.Is(err, backend.ErrConflict) {
				lastErr = err
				continue // re-read the winner's state and retry.
			}
			return fmt.Errorf("fence: put %s: %w", key, err)
		}
		return nil
	}
	return fmt.Errorf("%w: exhausted %d attempts fencing %s at round %d", lastErr, maxCASAttempts, key, round)
}

// Claim is the safe TAKEOVER seal: it reads the latest durably-stored payload for
// key and re-seals it UNCHANGED at round, invoking hydrate on that payload first.
// It exists because a successor must never seal a payload it read BEFORE a
// predecessor's last landed write, or that acknowledged write would be clobbered.
// The read and the seal are one compare-and-set on the same version: if a
// predecessor write lands in between, the CAS conflicts and Claim retries,
// re-reading the newer payload, re-hydrating, and re-sealing IT. After Claim
// returns, every predecessor write at a round below `round` is fenced. If a retry
// finds exactly the record it wrote, its first attempt landed and the reply was
// lost: that is success, not a lost race.
//
// hydrate receives the payload and the round recorded with it, and may refuse the
// takeover by returning an error (Claim returns it and writes nothing). It may run
// more than once across retries, so it must be idempotent. An absent key is
// recorded round 0 with a nil payload.
func (s *Store) Claim(ctx context.Context, key string, round uint64, hydrate func(payload []byte, recorded uint64) error) error {
	return s.Rewrite(ctx, key, round, func(payload []byte, recorded uint64) ([]byte, error) {
		if hydrate != nil {
			if err := hydrate(payload, recorded); err != nil {
				return nil, err
			}
		}
		return payload, nil
	})
}

// Rewrite is Claim that may change the payload: carry receives the payload read and
// the round recorded with it and returns the payload to record in its place. A claim
// carries it unchanged; sealing a Head for another place changes it. A retry that
// finds exactly the bytes of the attempt before it knows that attempt landed, so
// carry may return different bytes each time it runs (fresh randomness in a sealed
// payload) and the lost-reply case is still recognised within one call. A caller
// that retries after an error that is not a lost race starts over, and a record it
// wrote earlier under its token is not its own any more: it must use a higher round.
func (s *Store) Rewrite(ctx context.Context, key string, round uint64, carry func(payload []byte, recorded uint64) ([]byte, error)) error {
	if s == nil || s.be == nil {
		return fmt.Errorf("fence: nil store")
	}
	var lastErr error
	var last []byte
	for range maxCASAttempts {
		data, version, err := s.read(ctx, key)
		if err != nil {
			return fmt.Errorf("fence: claim read %s: %w", key, err)
		}
		if last != nil && bytes.Equal(data, last) {
			return nil
		}
		var recorded uint64
		var payload []byte
		if data != nil {
			if recorded, payload, err = decode(data); err != nil {
				return err
			}
		}
		if round < recorded {
			return fmt.Errorf("%w: key=%s round=%d recorded=%d", ErrStaleRound, key, round, recorded)
		}
		if carry != nil {
			if payload, err = carry(payload, recorded); err != nil {
				return fmt.Errorf("fence: claim %s: %w", key, err)
			}
		}
		last = s.encode(round, payload)
		if err := s.fits(last); err != nil {
			return fmt.Errorf("fence: claim %s: %w", key, err)
		}
		if err := s.write(ctx, key, last, version, round); err != nil {
			if errors.Is(err, backend.ErrConflict) {
				lastErr = err
				continue // a predecessor's write landed; re-read it and carry it forward.
			}
			return fmt.Errorf("fence: claim seal %s: %w", key, err)
		}
		return nil
	}
	return fmt.Errorf("%w: exhausted %d attempts claiming %s at round %d", lastErr, maxCASAttempts, key, round)
}

// Get returns the fenced payload and the round it was written at, or an error
// wrapping backend.ErrNotFound when key is absent.
func (s *Store) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	data, _, err := s.be.Get(ctx, key, s.max)
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
// absent: the value a new owner exceeds to claim the writer role.
func (s *Store) Round(ctx context.Context, key string) (uint64, error) {
	data, _, err := s.read(ctx, key)
	if err != nil || data == nil {
		return 0, err
	}
	round, _, err := decode(data)
	return round, err
}
