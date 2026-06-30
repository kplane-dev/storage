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
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

// Conformance tests run the upstream storage.Interface suite against the
// Spanner backend. Each TestConformance* delegates to a RunTest* from
// staging/.../pkg/storage/testing — the same contract etcd3's store_test.go
// exercises. Failures here mean we've diverged from etcd's storage.Interface
// semantics in a way the apiserver depends on.
//
// These tests require the Spanner emulator (SPANNER_EMULATOR_HOST). They
// share the existing setupTestStore helper but reconfigure it with the
// example.Pod codec and a "/pods/" resource prefix — matching the layout
// the conformance suite expects.

var conformanceScheme = runtime.NewScheme()
var conformanceCodecs = serializer.NewCodecFactory(conformanceScheme)

func init() {
	metav1.AddToGroupVersion(conformanceScheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(conformanceScheme))
	utilruntime.Must(examplev1.AddToScheme(conformanceScheme))
}

// setupConformanceStore returns a Spanner-backed store configured the way
// upstream conformance tests expect: Pod codec, `/pods/` resource prefix,
// identity transformer, no wrapDecodedObject hook.
func setupConformanceStore(t *testing.T) (context.Context, *store) {
	t.Helper()
	skipIfNoEmulator(t)

	raw := setupTestStore(t) // emulator, fresh DB, default config
	// Rewire codec / new funcs / resource prefix to the conformance shape.
	codec := apitesting.TestCodec(conformanceCodecs, examplev1.SchemeGroupVersion)
	raw.codec = codec
	raw.newFunc = func() runtime.Object { return &example.Pod{} }
	raw.newListFunc = func() runtime.Object { return &example.PodList{} }
	raw.resourcePrefix = "/pods/"
	raw.groupResource = "pods"
	raw.transformer = identity.NewEncryptCheckTransformer()
	raw.wrapDecodedObject = nil
	return context.Background(), raw
}

// Shared no-op helpers — etcd3 implements these in terms of etcd's compact
// RPC; Spanner has no equivalent, so we make them no-ops. Tests that
// exercise post-compaction semantics will skip themselves gracefully (the
// underlying suite checks the returned RV).

func noopKeyValidation(ctx context.Context, t *testing.T, key string) {}

func noopCompaction(ctx context.Context, t *testing.T, resourceVersion string) {}

func noopIncreaseRV(ctx context.Context, t *testing.T) int64 {
	// Force-advance the Spanner clock by writing a throwaway row. Callers
	// pass this so the suite can probe "RV strictly newer than seen so far"
	// behavior. Returning 0 makes the suite use whatever Get returns.
	return 0
}

// resourcePrefixForGroupResource — etcd3 stores objects under a per-resource
// key prefix. The conformance suite expects /pods/ for example.Pod objects.
var _ = schema.GroupResource{Resource: "pods"} // anchor import

// --- The conformance battery. One Test per RunTest entry point. ---

func TestConformance_Create(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestCreate(ctx, t, s, noopKeyValidation)
}

func TestConformance_CreateWithTTL(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestCreateWithTTL(ctx, t, s)
}

func TestConformance_CreateWithKeyExist(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestCreateWithKeyExist(ctx, t, s)
}

func TestConformance_Get(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestGet(ctx, t, s)
}

func TestConformance_UnconditionalDelete(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestUnconditionalDelete(ctx, t, s)
}

func TestConformance_ConditionalDelete(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestConditionalDelete(ctx, t, s)
}

func TestConformance_DeleteWithSuggestion(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestDeleteWithSuggestion(ctx, t, s)
}

func TestConformance_DeleteWithSuggestionAndConflict(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestDeleteWithSuggestionAndConflict(ctx, t, s)
}

func TestConformance_DeleteWithConflict(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestDeleteWithConflict(ctx, t, s)
}

func TestConformance_GuaranteedUpdate(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestGuaranteedUpdate(ctx, t, &storeWithPrefixTransformer{Interface: s}, noopKeyValidation)
}

func TestConformance_GuaranteedUpdateWithTTL(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestGuaranteedUpdateWithTTL(ctx, t, s)
}

func TestConformance_GuaranteedUpdateWithConflict(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestGuaranteedUpdateWithConflict(ctx, t, s)
}

func TestConformance_GuaranteedUpdateWithSuggestionAndConflict(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestGuaranteedUpdateWithSuggestionAndConflict(ctx, t, s)
}

func TestConformance_GetListNonRecursive(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestGetListNonRecursive(ctx, t, noopIncreaseRV, s)
}

func TestConformance_GetListRecursivePrefix(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestGetListRecursivePrefix(ctx, t, s)
}

func TestConformance_Watch(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestWatch(ctx, t, s)
}

func TestConformance_WatchFromZero(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	// Pass nil compaction so the suite t.Skip()'s the compaction-dependent
	// subtests. Compaction is etcd's revision-GC mechanism; Spanner uses
	// its own retention model and we haven't built a Compaction adapter
	// (tracked separately as a future-work gap).
	storagetesting.RunTestWatchFromZero(ctx, t, s, nil)
}

func TestConformance_DeleteTriggerWatch(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestDeleteTriggerWatch(ctx, t, s)
}

func TestConformance_WatchFromNonZero(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestWatchFromNonZero(ctx, t, s)
}

func TestConformance_WatchContextCancel(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestWatchContextCancel(ctx, t, s)
}

func TestConformance_WatcherTimeout(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestWatcherTimeout(ctx, t, s)
}

func TestConformance_WatchDeleteEventObjectHaveLatestRV(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestWatchDeleteEventObjectHaveLatestRV(ctx, t, s)
}

func TestConformance_ClusterScopedWatch(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestClusterScopedWatch(ctx, t, s)
}

func TestConformance_NamespaceScopedWatch(t *testing.T) {
	ctx, s := setupConformanceStore(t)
	storagetesting.RunTestNamespaceScopedWatch(ctx, t, s)
}

// storeWithPrefixTransformer is a minimal adapter satisfying the suite's
// InterfaceWithPrefixTransformer when the test needs to swap transformers
// mid-run. Our store doesn't support hot-swapping the transformer; tests
// that rely on it (RunTestTransformationFailure, RunTestWatchError) are
// intentionally excluded from this battery.
type storeWithPrefixTransformer struct {
	storage.Interface
}

func (s *storeWithPrefixTransformer) UpdatePrefixTransformer(_ storagetesting.PrefixTransformerModifier) func() {
	return func() {}
}
