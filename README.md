# kplane-dev/storage

Cluster-aware storage primitives for multicluster Kubernetes API servers.

This library bridges the [kplane-dev/kubernetes](https://github.com/kplane-dev/kubernetes) fork (which adds cluster identity hooks to the Kubernetes cacher pipeline) with a clean API for consumers that need to route, filter, or fan out objects by cluster.

## How It Works

kplane embeds cluster identity in etcd key paths:

```
/<etcdPrefix>/<resourcePrefix>/clusters/<clusterID>/[<namespace>/]<name>
```

This layout allows a single recursive etcd watch per resource type to capture events for **all** clusters. The identity system extracts the cluster ID from the key at the cacher layer and propagates it through watch events — without modifying any Kubernetes objects visible to API clients.

## Key Components

### ObjectWithClusterIdentity

An envelope type that wraps a `runtime.Object` with its cluster ID and storage key. Used as the `watch.Event.Object` type when identity hooks are configured.

```go
type ObjectWithClusterIdentity struct {
    Object     runtime.Object
    ClusterID  string
    StorageKey string
}
```

### UnwrapClusterIdentity

Consumer-facing helper that extracts the inner object and cluster ID. Handles both wrapped and unwrapped objects.

```go
inner, clusterID := storage.UnwrapClusterIdentity(event.Object)
// inner is the original *v1.Pod (or whatever type)
// clusterID is "c1", "c2", etc. (empty if not wrapped)
```

### KeyLayout

Describes how cluster identity is embedded in storage key paths. Provides key parsing and prefix generation utilities.

```go
kl := storage.DefaultKeyLayout() // ClusterSegment: "clusters"

kl.ClusterFromKey("/pods/clusters/c1/default/nginx")  // → "c1"
kl.KindRootPrefix("/pods")                            // → "/pods/clusters/"
kl.PerClusterPrefix("/pods", "c1")                    // → "/pods/clusters/c1/"
```

### StorageWithClusterIdentity

Drop-in replacement for `registry.StorageWithCacher()` that configures the cacher with identity hooks.

```go
decorator := storage.StorageWithClusterIdentity(storage.DecoratorConfig{
    KeyLayout:     storage.DefaultKeyLayout(),
    GroupResource: schema.GroupResource{Resource: "pods"},
})
```

## Usage

Wire `StorageWithClusterIdentity` into your apiserver's `RESTOptionsGetter` where you would normally use `registry.StorageWithCacher()`. Watch consumers then call `UnwrapClusterIdentity()` on each event to extract the cluster ID and route accordingly.

```go
// In your apiserver setup:
decorator := storage.StorageWithClusterIdentity(storage.DecoratorConfig{
    KeyLayout:     storage.DefaultKeyLayout(),
    GroupResource: gr,
})

// In your watch consumer:
for event := range watcher.ResultChan() {
    inner, clusterID := storage.UnwrapClusterIdentity(event.Object)
    // Route inner object to the correct cluster handler
}
```

## Design Principles

- **Zero client-visible metadata**: no labels, annotations, or API field changes on objects returned to clients
- **Identity from keys only**: cluster ID is derived purely from the storage key path, not from object contents
- **Nil-safe hooks**: when identity hooks are not configured, behavior is identical to upstream Kubernetes
- **Backend agnostic**: works with any `storage.Interface` implementation (etcd, TiKV, etc.)

## Requirements

Requires the [kplane-dev/kubernetes](https://github.com/kplane-dev/kubernetes) fork which adds `IdentityFromKey` and `WrapWatchObject` hooks to the cacher `Config`.

## Testing

```bash
# Unit tests (no dependencies)
go test -run "TestObjectWithClusterIdentity|TestUnwrapClusterIdentity|TestKeyLayout" -v

# E2E tests (requires etcd binary)
go test -run "TestE2E" -v
```

See the [design document](https://github.com/kplane-dev/kubernetes/blob/master/docs/design-multicluster-storage-identity.md) for full architecture details.
