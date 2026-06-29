// Package registry hosts the kplane storage backend registry: an
// instance-scoped lookup of named backends (etcd3, spanner, future
// postgres, etc.) that the apiserver consults instead of the hardcoded
// per-backend if/else it carries today.
//
// Design lives in KPEP-0001 (kplane-dev/enhancements). The shape mirrors
// upstream admission.Plugins: backends self-describe via the Backend
// interface, register against a *Backends, and the apiserver picks one
// at startup based on --storage-backend.
//
// This package is intentionally additive — nothing in the rest of
// kplane-dev/storage imports it. Existing consumers continue to use
// DecoratorConfig / StorageWithClusterIdentity unchanged. Wiring happens
// in the apiserver once the per-backend Register hooks are in place.
package registry

import (
	"fmt"
	"sort"
	"sync"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
)

// Factory is the per-resource constructor a backend returns from Build().
// The signature is a 1-to-1 match with upstream's
// storagebackend/factory.Create so any backend that already satisfies the
// existing kplane-dev/spanner BackendFactory shape works without
// adaptation. The apiserver's RESTOptionsGetter calls this once per
// GroupResource at REST registry construction time.
type Factory func(
	config *storagebackend.ConfigForResource,
	newFunc, newListFunc func() runtime.Object,
	resourcePrefix string,
) (storage.Interface, factory.DestroyFunc, error)

// Backend is what every registered storage backend implements. The
// lifecycle matches upstream's options-struct convention: bind flags,
// validate after parse, build the per-resource factory once.
//
// AddFlags is always called for every registered backend so the apiserver
// help output lists every backend's flag block, the same way upstream
// surfaces --etcd-* / --storage-media-type / --watch-cache regardless of
// which storage backend is selected.
//
// Validate is called only for the backend whose Name() matches the
// selected --storage-backend value. Build is called once after Validate
// passes and returns the per-resource Factory threaded into the apiserver's
// RESTOptions decorator chain.
type Backend interface {
	// Name is the value matched against --storage-backend. The full set
	// of registered names is shown in the flag help text.
	Name() string

	// AddFlags binds the backend's CLI flags to fs. Called for every
	// registered backend at startup so all flags appear in --help.
	// In-tree backends are free to make this a no-op when their flags
	// are already bound by another options block (the etcd wrapper does
	// this because upstream EtcdOptions already owns --etcd-*).
	AddFlags(fs *pflag.FlagSet)

	// Validate runs the backend's flag-level validation. Only called for
	// the selected backend, after flag parsing. Errors short-circuit
	// apiserver startup with a clear message naming the backend.
	Validate() []error

	// Build returns the per-resource Factory. Called once after Validate.
	// Any expensive setup (dialing a remote, opening a session pool)
	// belongs here, not in AddFlags or Validate.
	Build() (Factory, error)

	// BuildFactoryBackend returns the fork-level factory.Backend that the
	// apiserver registers via factory.Register(name, b) so non-CR storage
	// callsites (master/peer endpoint leases, service IP/NodePort
	// allocators) dispatch to this backend the same way CR storage does.
	//
	// Implementations should reuse the same underlying client/state as
	// Build() to avoid double-dialing — typical pattern is to build state
	// once internally and have both Build and BuildFactoryBackend hand out
	// thin views over it.
	BuildFactoryBackend() (factory.Backend, error)
}

// Backends is the instance-scoped registry. It mirrors upstream's
// admission.Plugins shape — instance-scoped rather than a package-level
// global, so tests can construct fresh registries without leaking state
// and external consumers can build custom apiservers with their own
// backend selections.
type Backends struct {
	mu         sync.RWMutex
	registered map[string]Backend
}

// New returns an empty Backends registry. The apiserver constructs one
// in main, calls RegisterBuiltin (from kplane-dev/storage/backends), and
// optionally adds external backends before threading it into the options
// chain.
func New() *Backends {
	return &Backends{registered: map[string]Backend{}}
}

// Register installs backend into b, keyed by backend.Name(). Panics on
// duplicate name — same convention as database/sql.Register: a duplicate
// registration is a programming error, not a runtime condition we want
// to silently overwrite.
func (b *Backends) Register(backend Backend) {
	name := backend.Name()
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.registered[name]; exists {
		panic(fmt.Sprintf("storage backend %q already registered", name))
	}
	b.registered[name] = backend
}

// Get returns the backend registered under name, or ok=false if no such
// backend was registered. The apiserver uses this after flag parse to
// dispatch Validate/Build.
func (b *Backends) Get(name string) (Backend, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	backend, ok := b.registered[name]
	return backend, ok
}

// Names returns the sorted list of registered backend names. Used by the
// apiserver to populate the --storage-backend flag help text and to
// produce a useful error when an unknown backend name is requested.
func (b *Backends) Names() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.registered))
	for n := range b.registered {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// AddFlags calls AddFlags on every registered backend, threading the same
// pflag set into each. Called once at startup before flag parse so the
// apiserver's --help lists every backend's flag block.
func (b *Backends) AddFlags(fs *pflag.FlagSet) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, backend := range b.registered {
		backend.AddFlags(fs)
	}
}
