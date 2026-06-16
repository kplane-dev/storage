package registry_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"

	"github.com/kplane-dev/storage/registry"
)

// fakeBackend is the minimal Backend impl used to exercise the registry.
// Real backends (etcd, spanner, postgres) live in
// kplane-dev/storage/backends/<name>/ in later PRs.
type fakeBackend struct {
	name           string
	flagsAdded     bool
	validateCalls  int
	validateErrors []error
	buildCalls     int
	buildErr       error
}

func (f *fakeBackend) Name() string                { return f.name }
func (f *fakeBackend) AddFlags(*pflag.FlagSet)     { f.flagsAdded = true }
func (f *fakeBackend) Validate() []error           { f.validateCalls++; return f.validateErrors }
func (f *fakeBackend) Build() (registry.Factory, error) {
	f.buildCalls++
	if f.buildErr != nil {
		return nil, f.buildErr
	}
	return func(*storagebackend.ConfigForResource, func() runtime.Object, func() runtime.Object, string) (storage.Interface, factory.DestroyFunc, error) {
		return nil, func() {}, nil
	}, nil
}

func TestRegisterAndGet(t *testing.T) {
	b := registry.New()
	a := &fakeBackend{name: "alpha"}
	b.Register(a)

	got, ok := b.Get("alpha")
	if !ok {
		t.Fatalf("Get(alpha) returned ok=false; want true")
	}
	if got != a {
		t.Fatalf("Get(alpha) returned %v; want %v", got, a)
	}
	if _, ok := b.Get("missing"); ok {
		t.Fatalf("Get(missing) returned ok=true; want false")
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	b := registry.New()
	b.Register(&fakeBackend{name: "alpha"})
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate register; got none")
		}
	}()
	b.Register(&fakeBackend{name: "alpha"})
}

func TestNamesSorted(t *testing.T) {
	b := registry.New()
	b.Register(&fakeBackend{name: "zeta"})
	b.Register(&fakeBackend{name: "alpha"})
	b.Register(&fakeBackend{name: "mu"})

	got := b.Names()
	want := []string{"alpha", "mu", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Names() = %v; want %v", got, want)
	}
}

func TestAddFlagsFanOut(t *testing.T) {
	b := registry.New()
	a, c := &fakeBackend{name: "alpha"}, &fakeBackend{name: "charlie"}
	b.Register(a)
	b.Register(c)

	b.AddFlags(pflag.NewFlagSet("test", pflag.ContinueOnError))

	if !a.flagsAdded || !c.flagsAdded {
		t.Fatalf("AddFlags didn't fan out: alpha=%v charlie=%v", a.flagsAdded, c.flagsAdded)
	}
}

func TestValidateAndBuildReachBackend(t *testing.T) {
	b := registry.New()
	want := errors.New("bad config")
	a := &fakeBackend{name: "alpha", validateErrors: []error{want}}
	b.Register(a)

	got, _ := b.Get("alpha")
	errs := got.Validate()
	if a.validateCalls != 1 {
		t.Fatalf("Validate not called on backend (calls=%d)", a.validateCalls)
	}
	if len(errs) != 1 || !errors.Is(errs[0], want) {
		t.Fatalf("Validate returned %v; want [%v]", errs, want)
	}

	if _, err := got.Build(); err != nil {
		t.Fatalf("Build returned err=%v; want nil", err)
	}
	if a.buildCalls != 1 {
		t.Fatalf("Build not called on backend (calls=%d)", a.buildCalls)
	}
}
