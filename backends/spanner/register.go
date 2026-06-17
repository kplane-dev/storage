package spanner

import (
	"fmt"

	"github.com/spf13/pflag"

	"github.com/kplane-dev/storage/registry"
)

// Options is the Spanner backend's flag block. It implements
// registry.Backend so an apiserver constructing a *registry.Backends can
// register Spanner via NewOptions() (and switch to it at runtime via
// --storage-backend=spanner) without the apiserver knowing anything
// Spanner-specific.
//
// Lifecycle:
//   - AddFlags binds the --spanner-* flags. Called for every registered
//     backend at startup so the help text lists every backend's flags.
//   - Validate runs flag-level checks (required fields, sensible
//     combinations). Called only when --storage-backend=spanner.
//   - Build dials the shared Spanner client and returns a registry.Factory
//     that creates per-resource stores against it.
//
// The Build factory delegates to NewBackendFactory (the existing
// constructor used by the apiserver's pre-registry hardcoded path), so the
// underlying store, broadcaster, and watcher behavior is identical between
// the legacy --spanner-project dispatch and the new --storage-backend
// dispatch. That lets us flip between paths during the cutover without
// behavior drift.
type Options struct {
	cfg SpannerConfig
}

// NewOptions returns a fresh Options. The apiserver registers this via
// b.Register(spanner.NewOptions()) inside its backends/RegisterBuiltin
// aggregator.
func NewOptions() *Options {
	return &Options{}
}

// Name is the value matched against --storage-backend.
func (o *Options) Name() string { return "spanner" }

// AddFlags binds the --spanner-* flags. The flag set is shared across all
// registered backends so the apiserver's --help lists every backend's
// flags regardless of which one is selected.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.cfg.Project, "spanner-project", o.cfg.Project,
		"Google Cloud project hosting the Spanner instance.")
	fs.StringVar(&o.cfg.Instance, "spanner-instance", o.cfg.Instance,
		"Spanner instance ID.")
	fs.StringVar(&o.cfg.Database, "spanner-database", o.cfg.Database,
		"Spanner database name within the instance.")
	fs.StringVar(&o.cfg.EmulatorHost, "spanner-emulator-host", o.cfg.EmulatorHost,
		"Optional host:port of a local Spanner emulator. When set, the project/instance/database flags still apply but credentials are skipped.")
}

// Validate runs flag-level checks. Only invoked for the selected backend,
// so users running with --storage-backend=etcd3 (or the legacy default)
// aren't forced to set Spanner flags.
func (o *Options) Validate() []error {
	var errs []error
	if o.cfg.Project == "" {
		errs = append(errs, fmt.Errorf("--spanner-project is required when --storage-backend=spanner"))
	}
	if o.cfg.Instance == "" {
		errs = append(errs, fmt.Errorf("--spanner-instance is required when --storage-backend=spanner"))
	}
	if o.cfg.Database == "" {
		errs = append(errs, fmt.Errorf("--spanner-database is required when --storage-backend=spanner"))
	}
	return errs
}

// Build returns the per-resource Factory. The apiserver calls this once
// after Validate; the resulting Factory is invoked once per GroupResource
// at REST registry construction time.
//
// We reuse the existing NewBackendFactory so the legacy hardcoded path
// (apiserver's `if opts.SpannerProject != ""` branch) and the registry
// path produce identical store/broadcaster/watcher behavior. That keeps
// the Phase 3 cutover in the apiserver behavior-preserving.
func (o *Options) Build() (registry.Factory, error) {
	bf := NewBackendFactory(o.cfg)
	return registry.Factory(bf), nil
}

// Compile-time assertion: Options satisfies registry.Backend.
var _ registry.Backend = (*Options)(nil)

// Config exposes the resolved SpannerConfig for callers that need to
// inspect the backend's wiring (e.g. health-probe construction, debug
// endpoints). Returns the value, not a pointer, so callers can't mutate
// the live config after Build.
func (o *Options) Config() SpannerConfig { return o.cfg }
