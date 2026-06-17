// Package backends is the aggregator: a single import the apiserver pulls
// in to register every in-tree storage backend against a *registry.Backends.
//
// Pattern mirrors upstream kubeapiserver/options/plugins.go: the apiserver
// imports this one package; the package imports each backend; each backend
// self-describes via its Options struct. Adding a backend (postgres, kine,
// etc.) means one new import + one Register() call here — no apiserver
// change required.
package backends

import (
	"github.com/kplane-dev/storage/backends/spanner"
	"github.com/kplane-dev/storage/registry"
)

// RegisterBuiltin installs the in-tree backends into b. The apiserver
// constructs a *registry.Backends in main, calls RegisterBuiltin, then
// hands the registry to its options chain. External backends (out-of-tree)
// would call b.Register(<their>.NewOptions()) directly from a custom
// apiserver main, after RegisterBuiltin.
func RegisterBuiltin(b *registry.Backends) {
	b.Register(spanner.NewOptions())
	// Future backends:
	//   b.Register(postgres.NewOptions())
	//   b.Register(kine.NewOptions())
}
