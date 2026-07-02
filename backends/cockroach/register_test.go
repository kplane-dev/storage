package cockroach

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/kplane-dev/storage/registry"
)

func TestOptions_Name(t *testing.T) {
	if got := NewOptions().Name(); got != "cockroach" {
		t.Errorf("Name = %q, want cockroach", got)
	}
}

func TestOptions_AddFlags_BindsExpected(t *testing.T) {
	o := NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	for _, name := range []string{
		"cockroach-dsn",
		"cockroach-database",
		"cockroach-max-conns",
		"cockroach-max-conn-lifetime",
		"cockroach-max-conn-idle-time",
		"cockroach-health-check-period",
	} {
		if f := fs.Lookup(name); f == nil {
			t.Errorf("--%s not registered", name)
		}
	}
}

func TestOptions_Validate_RequiresDSN(t *testing.T) {
	o := NewOptions()
	if errs := o.Validate(); len(errs) == 0 {
		t.Errorf("expected validation error for empty DSN")
	}
	o.cfg.DSN = "postgres://x@y:26257/z?sslmode=disable"
	if errs := o.Validate(); len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
}

func TestOptions_SatisfiesRegistryBackend(t *testing.T) {
	// Compile-time check: Options implements registry.Backend.
	var _ registry.Backend = (*Options)(nil)

	// Runtime check: Register can accept it.
	b := registry.New()
	b.Register(NewOptions())
	if _, ok := b.Get("cockroach"); !ok {
		t.Errorf("backend not found after Register")
	}
}

func TestOptions_Build_RequiresValidConfig(t *testing.T) {
	o := NewOptions()
	o.cfg.DSN = "postgres://user@unreachable-host:26257/db?sslmode=disable&connect_timeout=1"
	if _, err := o.Build(); err == nil {
		t.Errorf("expected Build to fail against unreachable DSN")
	}
}
