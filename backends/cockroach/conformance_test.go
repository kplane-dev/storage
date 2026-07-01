package cockroach

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

// Conformance tests run the upstream storage.Interface suite against the
// CockroachDB backend. Each TestConformance* delegates to a RunTest*
// from staging/.../pkg/storage/testing — the same contract etcd3 and
// Spanner ship against.

var conformanceScheme = runtime.NewScheme()
var conformanceCodecs = serializer.NewCodecFactory(conformanceScheme)

func init() {
	metav1.AddToGroupVersion(conformanceScheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(conformanceScheme))
	utilruntime.Must(examplev1.AddToScheme(conformanceScheme))
}

// setupConformanceStore returns a Cockroach-backed store configured the
// way upstream conformance tests expect: Pod codec, `/pods/` resource
// prefix, identity transformer, no wrapDecodedObject hook. Wired to a
// live ChangefeedSubscription so watch tests have real events.
func setupConformanceStore(t *testing.T) (context.Context, *store) {
	t.Helper()
	skipIfNoCockroach(t)

	raw := setupTestStore(t)
	codec := apitesting.TestCodec(conformanceCodecs, examplev1.SchemeGroupVersion)
	raw.codec = codec
	raw.newFunc = func() runtime.Object { return &example.Pod{} }
	raw.newListFunc = func() runtime.Object { return &example.PodList{} }
	raw.resourcePrefix = "/pods/"
	raw.groupResource = "pods"
	raw.transformer = identity.NewEncryptCheckTransformer()
	raw.wrapDecodedObject = nil

	cc, err := (Config{DSN: testDSN(), Database: currentDatabase(t, raw)}).ConnConfig()
	if err != nil {
		t.Fatalf("ConnConfig: %v", err)
	}
	cf := NewChangefeedSubscription(cc)
	cf.Start(context.Background())
	raw.SetChangefeed(cf)
	t.Cleanup(cf.Stop)
	waitForChangefeedReady(t, cf, 5*time.Second)

	scanner := NewTTLScanner(raw, 0)
	scanner.Start(context.Background())
	t.Cleanup(scanner.Stop)

	return context.Background(), raw
}

// Suite-required hooks. Cockroach's HLC and MVCC subsume compaction; the
// suite's compaction-dependent subtests self-skip when compaction is nil.

func noopKeyValidation(ctx context.Context, t *testing.T, key string) {}

func noopIncreaseRV(ctx context.Context, t *testing.T) int64 { return 0 }

// waitForChangefeedReady blocks until the subscription has emitted at
// least one resolved timestamp, meaning the underlying pgx conn is
// established and the server-side changefeed job is streaming.
func waitForChangefeedReady(t *testing.T, cf *ChangefeedSubscription, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cf.ResolvedHLC() != "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("changefeed never reached ready state within %v", d)
}

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
	// Nil compaction -> the suite skips compaction-dependent subtests.
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

// storeWithPrefixTransformer satisfies the suite's
// InterfaceWithPrefixTransformer when the test needs to swap transformers
// mid-run. We don't support hot-swap; the suite's transformer-swap tests
// (RunTestTransformationFailure, RunTestWatchError) are intentionally
// excluded from this battery.
type storeWithPrefixTransformer struct {
	storage.Interface
}

func (s *storeWithPrefixTransformer) UpdatePrefixTransformer(_ storagetesting.PrefixTransformerModifier) func() {
	return func() {}
}
