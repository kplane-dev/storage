package spanner_test

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/kplane-dev/storage/backends/spanner"
	"github.com/kplane-dev/storage/registry"
)

func TestOptionsImplementsBackend(t *testing.T) {
	// Compile-time check that *Options implements registry.Backend.
	// Repeated here as a runtime assertion so a future signature drift
	// fails the test instead of silently breaking the registry contract.
	var _ registry.Backend = spanner.NewOptions()

	if got := spanner.NewOptions().Name(); got != "spanner" {
		t.Fatalf("Name() = %q; want %q", got, "spanner")
	}
}

func TestAddFlagsBindsSpannerPrefixed(t *testing.T) {
	o := spanner.NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)

	want := []string{
		"spanner-project",
		"spanner-instance",
		"spanner-database",
		"spanner-emulator-host",
	}
	for _, name := range want {
		if fs.Lookup(name) == nil {
			t.Errorf("--%s not bound by AddFlags", name)
		}
	}
}

func TestValidateRequiresCoreFields(t *testing.T) {
	o := spanner.NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)

	errs := o.Validate()
	if len(errs) != 3 {
		t.Fatalf("Validate() returned %d errors; want 3 (missing project/instance/database). errs=%v", len(errs), errs)
	}
	for _, want := range []string{"spanner-project", "spanner-instance", "spanner-database"} {
		found := false
		for _, e := range errs {
			if strings.Contains(e.Error(), want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Validate() missing error mentioning --%s; got %v", want, errs)
		}
	}
}

func TestValidatePassesWhenFlagsSet(t *testing.T) {
	o := spanner.NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)

	if err := fs.Parse([]string{
		"--spanner-project=p",
		"--spanner-instance=i",
		"--spanner-database=d",
	}); err != nil {
		t.Fatalf("flag parse: %v", err)
	}

	if errs := o.Validate(); len(errs) != 0 {
		t.Fatalf("Validate() returned %v; want none", errs)
	}

	cfg := o.Config()
	if cfg.Project != "p" || cfg.Instance != "i" || cfg.Database != "d" {
		t.Fatalf("Config() did not capture flag values: %+v", cfg)
	}
}

func TestRegisterRoundtripsThroughRegistry(t *testing.T) {
	b := registry.New()
	b.Register(spanner.NewOptions())

	got, ok := b.Get("spanner")
	if !ok {
		t.Fatalf("Get(spanner) ok=false; want true")
	}
	if _, isOptions := got.(*spanner.Options); !isOptions {
		t.Fatalf("Get(spanner) returned %T; want *spanner.Options", got)
	}
}
