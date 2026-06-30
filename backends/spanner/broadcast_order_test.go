/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package spanner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPublishOrderMonotonic confirms that concurrent Create calls produce
// broadcaster events whose revisions are monotonically non-decreasing.
//
// The upstream cacher's watchCache requires monotonic resource versions:
// every event added to the cache must have an RV strictly greater than the
// previous event. When parallel writers commit at non-monotonic timestamps
// (Spanner assigns each commit a distinct TrueTime timestamp, but the
// goroutines that call Apply return in client-thread-scheduling order,
// which is independent of commit order), the broadcaster delivers events
// out of order. The cacher then drops or mis-orders them, which manifests
// downstream as missing watch events — admission lookups for namespaces
// that were just created fail until the cache eventually re-syncs.
//
// This test spawns N parallel Create calls and records every Publish in
// the order it reaches the broadcaster. It then asserts the recorded RVs
// are monotonic. Against the buggy implementation (Publish called inline
// from the worker goroutine), N≥8 reliably reproduces the regression. The
// fix orders Publish by commit timestamp.
func TestPublishOrderMonotonic(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const N = 40

	// Subscribe before issuing writes so we observe every delivered event.
	sub := s.broadcaster.Subscribe(64 * N)
	defer s.broadcaster.Unsubscribe(sub)

	// Drain events into a slice on a background goroutine. Done when N
	// events have arrived OR a deadline expires.
	type captured struct {
		rev int64
		key string
	}
	captureCh := make(chan captured, N)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case e, ok := <-sub.ch:
				if !ok {
					return
				}
				captureCh <- captured{rev: e.rev, key: e.key}
				if len(captureCh) == N {
					return
				}
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()

	// Fan out N Create calls in parallel.
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			obj := &testObj{
				TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("order-%03d", i), Namespace: "default"},
				Data:       fmt.Sprintf("payload-%d", i),
			}
			key := fmt.Sprintf("/testobjs/default/order-%03d", i)
			out := &testObj{}
			if err := s.Create(ctx, key, obj, out, 0); err != nil {
				t.Errorf("Create(%s): %v", key, err)
			}
		}(i)
	}
	wg.Wait()
	<-done

	close(captureCh)
	var events []captured
	for c := range captureCh {
		events = append(events, c)
	}
	if len(events) < N {
		t.Fatalf("captured %d events; want %d (broadcaster dropped events under load)", len(events), N)
	}

	// Assert monotonic non-decreasing RVs.
	for i := 1; i < len(events); i++ {
		if events[i].rev < events[i-1].rev {
			// Print the entire sequence for diagnostic clarity.
			t.Logf("captured publish order (rev, key):")
			for j, e := range events {
				marker := ""
				if j > 0 && events[j].rev < events[j-1].rev {
					marker = "  ← REGRESSION"
				}
				t.Logf("  [%2d] rev=%d key=%s%s", j, e.rev, e.key, marker)
			}
			t.Fatalf("Publish revisions are non-monotonic at index %d: rev %d follows %d (gap %d)",
				i, events[i].rev, events[i-1].rev, events[i-1].rev-events[i].rev)
		}
	}
}
