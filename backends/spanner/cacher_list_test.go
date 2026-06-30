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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	apistorage "k8s.io/apiserver/pkg/storage"
	cacherstorage "k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/features"

	mcstorage "github.com/kplane-dev/storage"
)

// TestCacherListWithPreExistingObject creates the object BEFORE the cacher
// starts. The cacher's reflector should pick it up during initial sync.
// If LIST returns 0 here, the bug is in the initial-list path
// (sendInitialEvents → reflector consumption → watchCache population).
func TestCacherListWithPreExistingObject(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const resourcePrefix = "/testobjs/clusters"
	const clusterID = "root"
	const objName = "preexisting"
	storageKey := resourcePrefix + "/" + clusterID + "/" + objName

	// Create the object DIRECTLY via the store, BEFORE the cacher exists.
	obj := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: objName},
		Data:       "preexisting",
	}
	if err := s.Create(ctx, storageKey, obj, &testObj{}, 0); err != nil {
		t.Fatalf("store.Create: %v", err)
	}

	// Now start the cacher. Its reflector's initial sync should fetch the
	// pre-existing object via store.GetList (inside our watcher's
	// sendInitialEvents) and populate the watchCache.
	kl := mcstorage.DefaultKeyLayout()
	cacherConfig := cacherstorage.Config{
		Storage:             s,
		Versioner:           apistorage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "testobjs"},
		EventsHistoryWindow: cacherstorage.DefaultEventFreshDuration,
		ResourcePrefix:      resourcePrefix,
		KeyFunc: func(obj runtime.Object) (string, error) {
			if id, ok := obj.(*mcstorage.ObjectWithClusterIdentity); ok && id != nil && id.StorageKey != "" {
				return id.StorageKey, nil
			}
			acc, _ := metaAccessor(obj)
			return resourcePrefix + "/" + clusterID + "/" + acc.GetName(), nil
		},
		GetAttrsFunc: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			acc, _ := metaAccessor(obj)
			return labels.Set(acc.GetLabels()), fields.Set{"metadata.name": acc.GetName()}, nil
		},
		NewFunc:         func() runtime.Object { return &testObj{} },
		NewListFunc:     func() runtime.Object { return &testObjList{} },
		Codec:           testCodecs.LegacyCodec(schema.GroupVersion{Group: "test.io", Version: "v1"}),
		IdentityFromKey: kl.IdentityFromKey(),
		WrapWatchObject: func(object runtime.Object, key string, cid string) runtime.Object {
			return &mcstorage.ObjectWithClusterIdentity{Object: object, ClusterID: cid, StorageKey: key}
		},
	}
	cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
	if err != nil {
		t.Fatalf("NewCacherFromConfig: %v", err)
	}
	t.Cleanup(cacher.Stop)
	if utilfeature.DefaultFeatureGate.Enabled(features.ResilientWatchCacheInitialization) {
		if err := cacher.Wait(ctx); err != nil {
			t.Fatalf("cacher.Wait: %v", err)
		}
	}
	delegator := cacherstorage.NewCacheDelegator(cacher, s)
	t.Cleanup(delegator.Stop)

	list := &testObjList{}
	if err := delegator.GetList(ctx, resourcePrefix+"/", apistorage.ListOptions{
		ResourceVersion: "0",
		Recursive:       true,
		Predicate:       apistorage.Everything,
	}, list); err != nil {
		t.Fatalf("delegator.GetList: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("after preexisting + cacher init, got %d items; want 1", len(list.Items))
	}
}

// TestCacherListUnderConcurrentWrites stresses the broadcaster+cacher chain
// the way apiserver bootstrap does — many goroutines hammering Create at
// once. The production symptom is that some objects never appear in
// subsequent LIST calls. If the broadcaster's Ticket dispatcher only flushes
// when pendingTickets reaches zero, a steady stream of Creates could starve
// LIST forever; this test catches that.
func TestCacherListUnderConcurrentWrites(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	const resourcePrefix = "/testobjs/clusters"
	const clusterID = "root"

	kl := mcstorage.DefaultKeyLayout()
	cacherConfig := cacherstorage.Config{
		Storage:             s,
		Versioner:           apistorage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "testobjs"},
		EventsHistoryWindow: cacherstorage.DefaultEventFreshDuration,
		ResourcePrefix:      resourcePrefix,
		KeyFunc: func(obj runtime.Object) (string, error) {
			if id, ok := obj.(*mcstorage.ObjectWithClusterIdentity); ok && id != nil && id.StorageKey != "" {
				return id.StorageKey, nil
			}
			acc, _ := metaAccessor(obj)
			return resourcePrefix + "/" + clusterID + "/" + acc.GetName(), nil
		},
		GetAttrsFunc: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			acc, _ := metaAccessor(obj)
			return labels.Set(acc.GetLabels()), fields.Set{"metadata.name": acc.GetName()}, nil
		},
		NewFunc:         func() runtime.Object { return &testObj{} },
		NewListFunc:     func() runtime.Object { return &testObjList{} },
		Codec:           testCodecs.LegacyCodec(schema.GroupVersion{Group: "test.io", Version: "v1"}),
		IdentityFromKey: kl.IdentityFromKey(),
		WrapWatchObject: func(object runtime.Object, key string, cid string) runtime.Object {
			return &mcstorage.ObjectWithClusterIdentity{Object: object, ClusterID: cid, StorageKey: key}
		},
	}
	cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
	if err != nil {
		t.Fatalf("NewCacherFromConfig: %v", err)
	}
	t.Cleanup(cacher.Stop)
	if utilfeature.DefaultFeatureGate.Enabled(features.ResilientWatchCacheInitialization) {
		if err := cacher.Wait(ctx); err != nil {
			t.Fatalf("cacher.Wait: %v", err)
		}
	}
	delegator := cacherstorage.NewCacheDelegator(cacher, s)
	t.Cleanup(delegator.Stop)

	// Fan out N parallel Creates — mimicking apiserver bootstrap burst.
	const N = 40
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			obj := &testObj{
				TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
				ObjectMeta: metav1.ObjectMeta{Name: name(i)},
				Data:       "x",
			}
			key := resourcePrefix + "/" + clusterID + "/" + name(i)
			errs <- s.Create(ctx, key, obj, &testObj{}, 0)
		}(i)
	}
	for i := 0; i < N; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	// Wait up to 5s for the cacher to see all N. If the broadcaster gets
	// stuck waiting for pendingTickets to drain (e.g., a Ticket leak or a
	// dispatcher that requires a quiet period), this poll will time out.
	deadline := time.Now().Add(5 * time.Second)
	var last int
	for time.Now().Before(deadline) {
		list := &testObjList{}
		if err := delegator.GetList(ctx, resourcePrefix+"/", apistorage.ListOptions{
			ResourceVersion: "0",
			Recursive:       true,
			Predicate:       apistorage.Everything,
		}, list); err != nil {
			t.Fatalf("delegator.GetList: %v", err)
		}
		last = len(list.Items)
		if last >= N {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("after 5s, cacher LIST returned %d/%d items", last, N)
}

func name(i int) string { return fmt.Sprintf("obj-%03d", i) }

// TestCacherListAfterCreate reproduces the production bug where the apiserver's
// cacher returns an empty list even though the spanner store contains the
// object and the broadcaster delivered the create event.
//
// We wire the SAME components kplane wires in production:
//
//   spanner.store (our backend)
//     ↓ feeds
//   cacher with ResourcePrefix=/<resource>/clusters AND IdentityFromKey /
//     WrapWatchObject from kplane decorator
//
// Then we:
//  1. Create an object via the store at the multicluster-keyed path
//     (/<resource>/clusters/<cid>/<name>) — this matches what the apiserver
//     produces after the cluster-identity decorator rewrites keys.
//  2. Wait for the cacher's watchCache to absorb the event.
//  3. Call cacher.GetList with the SAME key the apiserver REST handler passes
//     (the kindRootPrefix, "/<resource>/clusters/").
//  4. Assert the list contains exactly one item.
//
// If the bug repros here, it's purely in the cacher↔spanner interaction; the
// real apiserver isn't required to demonstrate it.
func TestCacherListAfterCreate(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	// resourcePrefix mirrors what kplane's mc.baseDecorator passes to the
	// cacher for a cluster-scoped resource: "/<resource>/clusters".
	const resourcePrefix = "/testobjs/clusters"
	const clusterID = "root"
	const objName = "alpha"
	storageKey := resourcePrefix + "/" + clusterID + "/" + objName

	kl := mcstorage.DefaultKeyLayout()

	// Reconfigure the store so its pathPrefix matches the production setup
	// (apiserver wires this as "/registry"). The default setupTestStore uses
	// "/registry" already; we pass keys with the cluster-rewritten shape.
	cacherConfig := cacherstorage.Config{
		Storage:             s,
		Versioner:           apistorage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "testobjs"},
		EventsHistoryWindow: cacherstorage.DefaultEventFreshDuration,
		ResourcePrefix:      resourcePrefix,
		KeyFunc: func(obj runtime.Object) (string, error) {
			// Mirror kplane decorator: prefer the cluster-identity-keyed
			// storage key when present, else fall back to namespace/name.
			if id, ok := obj.(*mcstorage.ObjectWithClusterIdentity); ok && id != nil && id.StorageKey != "" {
				return id.StorageKey, nil
			}
			acc, err := metaAccessor(obj)
			if err != nil {
				return "", err
			}
			return resourcePrefix + "/" + clusterID + "/" + acc.GetName(), nil
		},
		GetAttrsFunc: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			acc, err := metaAccessor(obj)
			if err != nil {
				return nil, nil, err
			}
			return labels.Set(acc.GetLabels()), fields.Set{"metadata.name": acc.GetName()}, nil
		},
		NewFunc:         func() runtime.Object { return &testObj{} },
		NewListFunc:     func() runtime.Object { return &testObjList{} },
		Codec:           testCodecs.LegacyCodec(schema.GroupVersion{Group: "test.io", Version: "v1"}),
		IdentityFromKey: kl.IdentityFromKey(),
		WrapWatchObject: func(object runtime.Object, key string, cid string) runtime.Object {
			return &mcstorage.ObjectWithClusterIdentity{
				Object:     object,
				ClusterID:  cid,
				StorageKey: key,
			}
		},
	}
	cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
	if err != nil {
		t.Fatalf("NewCacherFromConfig: %v", err)
	}
	t.Cleanup(cacher.Stop)
	if utilfeature.DefaultFeatureGate.Enabled(features.ResilientWatchCacheInitialization) {
		if err := cacher.Wait(ctx); err != nil {
			t.Fatalf("cacher.Wait: %v", err)
		}
	}

	// Wrap in CacheDelegator like the production decorator does. The
	// delegator decides per-request whether to serve from the cacher or
	// delegate to raw storage.
	delegator := cacherstorage.NewCacheDelegator(cacher, s)
	t.Cleanup(delegator.Stop)

	// Create the object via the raw store at the cluster-rewritten key.
	obj := &testObj{
		TypeMeta:   metav1.TypeMeta{APIVersion: "test.io/v1", Kind: "TestObj"},
		ObjectMeta: metav1.ObjectMeta{Name: objName},
		Data:       "payload",
	}
	out := &testObj{}
	if err := s.Create(ctx, storageKey, obj, out, 0); err != nil {
		t.Fatalf("store.Create(%s): %v", storageKey, err)
	}

	// Give the cacher a beat to consume the broadcaster event and update
	// its watchCache.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		list := &testObjList{}
		if err := delegator.GetList(ctx, resourcePrefix+"/", apistorage.ListOptions{
			ResourceVersion: "0",
			Recursive:       true,
			Predicate:       apistorage.Everything,
		}, list); err != nil {
			t.Fatalf("cacher.GetList: %v", err)
		}
		if len(list.Items) == 1 {
			return // success
		}
		if len(list.Items) > 1 {
			t.Fatalf("cacher.GetList returned %d items; want 1", len(list.Items))
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Last attempt + diagnostic dump.
	list := &testObjList{}
	if err := cacher.GetList(ctx, resourcePrefix+"/", apistorage.ListOptions{
		ResourceVersion: "0",
		Recursive:       true,
		Predicate:       apistorage.Everything,
	}, list); err != nil {
		t.Fatalf("cacher.GetList (final): %v", err)
	}
	if len(list.Items) == 0 {
		// Also exercise the raw store to prove the row IS there — if this
		// passes while cacher.GetList returns empty, the bug is squarely
		// in the cacher/watchCache integration with our backend.
		direct := &testObjList{}
		if err := s.GetList(ctx, resourcePrefix+"/", apistorage.ListOptions{
			Recursive: true,
			Predicate: apistorage.Everything,
		}, direct); err != nil {
			t.Fatalf("store.GetList: %v", err)
		}
		t.Fatalf("cacher.GetList returned 0 items after 5s; store.GetList returned %d items — cacher-side bug", len(direct.Items))
	}
	t.Fatalf("cacher.GetList returned %d items; want 1", len(list.Items))
}

// Use upstream's meta accessor without importing the long path.
func metaAccessor(obj runtime.Object) (interface{ GetName() string; GetLabels() map[string]string; GetNamespace() string }, error) {
	type accessor interface {
		GetName() string
		GetLabels() map[string]string
		GetNamespace() string
	}
	a, ok := obj.(accessor)
	if !ok {
		// Fall back to apimachinery's meta helpers — but testObj already
		// satisfies the interface via embedded ObjectMeta.
		return nil, errBadObj
	}
	return a, nil
}

var errBadObj = newBadObjErr()

func newBadObjErr() error {
	return &badObj{}
}

type badObj struct{}

func (b *badObj) Error() string { return "object doesn't implement accessor" }
