package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	apistorage "k8s.io/apiserver/pkg/storage"
	cacherstorage "k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/storage/etcd3"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/clock"
)

var (
	testScheme = runtime.NewScheme()
	testCodecs = serializer.NewCodecFactory(testScheme)
)

func init() {
	metav1.AddToGroupVersion(testScheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(testScheme))
	utilruntime.Must(examplev1.AddToScheme(testScheme))
}

func newPod() runtime.Object     { return &example.Pod{} }
func newPodList() runtime.Object { return &example.PodList{} }

func getPodAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod, ok := obj.(*example.Pod)
	if !ok {
		return nil, nil, fmt.Errorf("not a pod")
	}
	fs := fields.Set{
		"metadata.name":      pod.Name,
		"metadata.namespace": pod.Namespace,
		"spec.nodeName":      pod.Spec.NodeName,
	}
	return labels.Set(pod.Labels), fs, nil
}

// clusterKeyFunc computes the cache key from the object.
// Key layout: /pods/clusters/<cluster>/<namespace>/<name>
// The cluster is carried in the pod's Labels["cluster"] for test purposes.
func clusterKeyFunc(prefix string) func(runtime.Object) (string, error) {
	return func(obj runtime.Object) (string, error) {
		pod, ok := obj.(*example.Pod)
		if !ok {
			return "", fmt.Errorf("not a pod")
		}
		cluster := pod.Labels["cluster"]
		if cluster == "" {
			cluster = "default"
		}
		return fmt.Sprintf("%s%s/%s/%s", prefix, cluster, pod.Namespace, pod.Name), nil
	}
}

func makePod(name, namespace, cluster string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"cluster": cluster},
		},
	}
}

// setupE2E creates an etcd-backed storage pipeline with cluster identity hooks.
// Returns the delegator (for user-facing operations), raw storage (for direct writes),
// and a cleanup function.
func setupE2E(t *testing.T) (context.Context, apistorage.Interface, apistorage.Interface, func()) {
	t.Helper()

	server, _ := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	versioner := apistorage.APIObjectVersioner{}
	codec := apitesting.TestCodec(testCodecs, examplev1.SchemeGroupVersion)
	compactor := etcd3.NewCompactor(server.V3Client.Client, 0, clock.RealClock{}, nil)
	t.Cleanup(compactor.Stop)

	prefix := "/pods/clusters/"
	rawStorage, err := etcd3.New(
		server.V3Client,
		compactor,
		codec,
		newPod,
		newPodList,
		etcd3testing.PathPrefix(),
		prefix,
		schema.GroupResource{Resource: "pods"},
		identity.NewEncryptCheckTransformer(),
		etcd3.NewDefaultLeaseManagerConfig(),
		etcd3.NewDefaultDecoder(codec, versioner),
		versioner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rawStorage.Close)

	// Inject list errors for relist testing (standard pattern from upstream)
	listErrors := 1
	if clientfeatures.FeatureGates().Enabled(clientfeatures.WatchListClient) {
		listErrors = 0
	}
	wrappedStorage := &storagetesting.StorageInjectingListErrors{
		Interface: rawStorage,
		Errors:    listErrors,
	}

	// Key layout for identity extraction
	kl := DefaultKeyLayout()

	cacherConfig := cacherstorage.Config{
		Storage:             wrappedStorage,
		Versioner:           apistorage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "pods"},
		EventsHistoryWindow: cacherstorage.DefaultEventFreshDuration,
		ResourcePrefix:      prefix,
		KeyFunc:             clusterKeyFunc(prefix),
		GetAttrsFunc:        getPodAttrs,
		NewFunc:             newPod,
		NewListFunc:         newPodList,
		Codec:               codec,
		// Cluster identity hooks from kplane-dev/storage:
		IdentityFromKey: kl.IdentityFromKey(),
		WrapWatchObject: func(object runtime.Object, key string, clusterID string) runtime.Object {
			return &ObjectWithClusterIdentity{
				Object:     object,
				ClusterID:  clusterID,
				StorageKey: key,
			}
		},
	}

	cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
	if err != nil {
		t.Fatalf("Failed to create cacher: %v", err)
	}
	ctx := context.Background()

	if err := wait.PollInfinite(100*time.Millisecond, wrappedStorage.ErrorsConsumed); err != nil {
		t.Fatalf("Failed to inject list errors: %v", err)
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.ResilientWatchCacheInitialization) {
		if err := cacher.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}

	delegator := cacherstorage.NewCacheDelegator(cacher, wrappedStorage)

	terminate := func() {
		delegator.Stop()
		cacher.Stop()
		server.Terminate(t)
	}

	return ctx, delegator, rawStorage, terminate
}

// TestE2E_WatchCarriesClusterIdentity is the primary e2e test. It proves that:
// 1. Objects written to different cluster key paths get correct ClusterID
// 2. Watch events carry ObjectWithClusterIdentity envelopes
// 3. The ClusterID matches the key layout
// 4. Multiple clusters don't cross-contaminate
func TestE2E_WatchCarriesClusterIdentity(t *testing.T) {
	ctx, delegator, rawStorage, terminate := setupE2E(t)
	defer terminate()

	// Write pods to two different clusters via raw storage
	pods := []struct {
		name, ns, cluster string
	}{
		{"nginx", "default", "c1"},
		{"redis", "default", "c1"},
		{"coredns", "kube-system", "c2"},
	}

	for _, p := range pods {
		pod := makePod(p.name, p.ns, p.cluster)
		key := fmt.Sprintf("/pods/clusters/%s/%s/%s", p.cluster, p.ns, p.name)
		if err := rawStorage.Create(ctx, key, pod, &example.Pod{}, 0); err != nil {
			t.Fatalf("Failed to create %s: %v", p.name, err)
		}
	}

	// Start watching all clusters
	w, err := delegator.Watch(ctx, "/pods/clusters/", apistorage.ListOptions{
		ResourceVersion: "0",
		Predicate:       apistorage.Everything,
		Recursive:       true,
	})
	if err != nil {
		t.Fatalf("Failed to start watch: %v", err)
	}
	defer w.Stop()

	// Collect watch events
	type eventResult struct {
		name      string
		clusterID string
		key       string
		eventType watch.EventType
	}
	results := make(map[string]eventResult)
	timeout := time.After(15 * time.Second)

	for len(results) < 3 {
		select {
		case evt, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("watch channel closed")
			}
			if evt.Type == watch.Bookmark {
				continue
			}

			// Verify the object is wrapped in ObjectWithClusterIdentity
			env, ok := evt.Object.(*ObjectWithClusterIdentity)
			if !ok {
				t.Fatalf("watch event object is %T, want *ObjectWithClusterIdentity", evt.Object)
			}

			// Extract pod name using meta.Accessor (works with cachingObject too)
			accessor, err := meta.Accessor(env.Object)
			if err != nil {
				t.Fatalf("could not get accessor: %v", err)
			}
			name := accessor.GetName()

			results[name] = eventResult{
				name:      name,
				clusterID: env.ClusterID,
				key:       env.StorageKey,
				eventType: evt.Type,
			}
		case <-timeout:
			t.Fatalf("timed out waiting for watch events, got %d of 3", len(results))
		}
	}

	// Verify each event has correct cluster identity
	expectations := map[string]string{
		"nginx":   "c1",
		"redis":   "c1",
		"coredns": "c2",
	}

	for name, expected := range expectations {
		result, ok := results[name]
		if !ok {
			t.Errorf("missing watch event for %s", name)
			continue
		}
		if result.clusterID != expected {
			t.Errorf("%s: ClusterID = %q, want %q", name, result.clusterID, expected)
		}
		if result.eventType != watch.Added {
			t.Errorf("%s: event type = %v, want Added", name, result.eventType)
		}

		// Verify the key contains the correct cluster
		expectedKeyPrefix := fmt.Sprintf("/pods/clusters/%s/", expected)
		if len(result.key) < len(expectedKeyPrefix) || result.key[:len(expectedKeyPrefix)] != expectedKeyPrefix {
			t.Errorf("%s: StorageKey = %q, should start with %q", name, result.key, expectedKeyPrefix)
		}
	}
}

// TestE2E_WatchModifyAndDeleteCarryIdentity verifies that Modified and Deleted
// events also carry cluster identity.
func TestE2E_WatchModifyAndDeleteCarryIdentity(t *testing.T) {
	ctx, delegator, rawStorage, terminate := setupE2E(t)
	defer terminate()

	// Create a pod
	pod := makePod("nginx", "default", "c1")
	out := &example.Pod{}
	if err := rawStorage.Create(ctx, "/pods/clusters/c1/default/nginx", pod, out, 0); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// Start watching
	w, err := delegator.Watch(ctx, "/pods/clusters/", apistorage.ListOptions{
		ResourceVersion: "0",
		Predicate:       apistorage.Everything,
		Recursive:       true,
	})
	if err != nil {
		t.Fatalf("Failed to start watch: %v", err)
	}
	defer w.Stop()

	expectEvent := func(expectedType watch.EventType) *ObjectWithClusterIdentity {
		timeout := time.After(10 * time.Second)
		for {
			select {
			case evt, ok := <-w.ResultChan():
				if !ok {
					t.Fatalf("watch closed waiting for %v", expectedType)
				}
				if evt.Type == watch.Bookmark {
					continue
				}
				if evt.Type != expectedType {
					continue
				}
				env, ok := evt.Object.(*ObjectWithClusterIdentity)
				if !ok {
					t.Fatalf("event object is %T, want *ObjectWithClusterIdentity", evt.Object)
				}
				return env
			case <-timeout:
				t.Fatalf("timed out waiting for %v event", expectedType)
			}
		}
	}

	// Wait for Added event
	addedEnv := expectEvent(watch.Added)
	if addedEnv.ClusterID != "c1" {
		t.Errorf("Added: ClusterID = %q, want c1", addedEnv.ClusterID)
	}

	// Update the pod
	if err := rawStorage.GuaranteedUpdate(ctx, "/pods/clusters/c1/default/nginx", &example.Pod{}, false, nil,
		func(input runtime.Object, _ apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
			p := input.(*example.Pod)
			p.Labels["updated"] = "true"
			return p, nil, nil
		}, out); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	modifiedEnv := expectEvent(watch.Modified)
	if modifiedEnv.ClusterID != "c1" {
		t.Errorf("Modified: ClusterID = %q, want c1", modifiedEnv.ClusterID)
	}

	// Delete the pod
	if err := rawStorage.Delete(ctx, "/pods/clusters/c1/default/nginx", &example.Pod{}, nil,
		apistorage.ValidateAllObjectFunc, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	deletedEnv := expectEvent(watch.Deleted)
	if deletedEnv.ClusterID != "c1" {
		t.Errorf("Deleted: ClusterID = %q, want c1", deletedEnv.ClusterID)
	}
}

// TestE2E_UnwrapClusterIdentity_Integration verifies that UnwrapClusterIdentity
// correctly handles objects from the real cacher pipeline.
func TestE2E_UnwrapClusterIdentity_Integration(t *testing.T) {
	ctx, delegator, rawStorage, terminate := setupE2E(t)
	defer terminate()

	pod := makePod("nginx", "default", "c1")
	if err := rawStorage.Create(ctx, "/pods/clusters/c1/default/nginx", pod, &example.Pod{}, 0); err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	w, err := delegator.Watch(ctx, "/pods/clusters/", apistorage.ListOptions{
		ResourceVersion: "0",
		Predicate:       apistorage.Everything,
		Recursive:       true,
	})
	if err != nil {
		t.Fatalf("Failed to start watch: %v", err)
	}
	defer w.Stop()

	timeout := time.After(10 * time.Second)
	for {
		select {
		case evt, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("watch closed")
			}
			if evt.Type == watch.Bookmark {
				continue
			}

			// Use UnwrapClusterIdentity — the primary API for consumers
			inner, clusterID := UnwrapClusterIdentity(evt.Object)
			if clusterID != "c1" {
				t.Errorf("UnwrapClusterIdentity: clusterID = %q, want c1", clusterID)
			}

			// Verify the inner object is usable via meta.Accessor
			accessor, err := meta.Accessor(inner)
			if err != nil {
				t.Fatalf("inner object not accessible: %v", err)
			}
			if accessor.GetName() != "nginx" {
				t.Errorf("inner object name = %q, want nginx", accessor.GetName())
			}
			if accessor.GetNamespace() != "default" {
				t.Errorf("inner object namespace = %q, want default", accessor.GetNamespace())
			}
			return
		case <-timeout:
			t.Fatal("timed out waiting for watch event")
		}
	}
}
