package storage

import (
	"testing"
)

func TestKeyLayout_IdentityFromKey(t *testing.T) {
	kl := DefaultKeyLayout()
	extract := kl.IdentityFromKey()

	tests := []struct {
		key      string
		expected string
	}{
		{"/pods/clusters/c1/default/nginx", "c1"},
		{"/pods/clusters/c2/kube-system/coredns", "c2"},
		{"/pods/clusters/my-cluster/default/app", "my-cluster"},
		{"/registry/pods/clusters/c-abc123/default/nginx", "c-abc123"},
		{"/pods/clusters/c1", "c1"},
		{"/pods/default/nginx", ""},
		{"", ""},
		{"/pods/clusters/", ""},
	}
	for _, tt := range tests {
		got := extract(tt.key)
		if got != tt.expected {
			t.Errorf("IdentityFromKey(%q) = %q, want %q", tt.key, got, tt.expected)
		}
	}
}

func TestKeyLayout_ClusterFromKey(t *testing.T) {
	kl := DefaultKeyLayout()

	if got := kl.ClusterFromKey("/pods/clusters/c1/default/nginx"); got != "c1" {
		t.Errorf("ClusterFromKey = %q, want %q", got, "c1")
	}
	if got := kl.ClusterFromKey("/pods/default/nginx"); got != "" {
		t.Errorf("ClusterFromKey = %q, want empty", got)
	}
}

func TestKeyLayout_KindRootPrefix(t *testing.T) {
	kl := DefaultKeyLayout()

	tests := []struct {
		resourcePrefix string
		expected       string
	}{
		{"/pods", "/pods/clusters/"},
		{"/pods/", "/pods/clusters/"},
		{"/registry/apps/deployments", "/registry/apps/deployments/clusters/"},
	}
	for _, tt := range tests {
		got := kl.KindRootPrefix(tt.resourcePrefix)
		if got != tt.expected {
			t.Errorf("KindRootPrefix(%q) = %q, want %q", tt.resourcePrefix, got, tt.expected)
		}
	}
}

func TestKeyLayout_PerClusterPrefix(t *testing.T) {
	kl := DefaultKeyLayout()

	tests := []struct {
		resourcePrefix string
		clusterID      string
		expected       string
	}{
		{"/pods", "c1", "/pods/clusters/c1/"},
		{"/pods/", "c2", "/pods/clusters/c2/"},
		{"/registry/apps/deployments", "my-cluster", "/registry/apps/deployments/clusters/my-cluster/"},
	}
	for _, tt := range tests {
		got := kl.PerClusterPrefix(tt.resourcePrefix, tt.clusterID)
		if got != tt.expected {
			t.Errorf("PerClusterPrefix(%q, %q) = %q, want %q", tt.resourcePrefix, tt.clusterID, got, tt.expected)
		}
	}
}

func TestKeyLayout_CustomSegment(t *testing.T) {
	kl := KeyLayout{ClusterSegment: "tenants"}
	extract := kl.IdentityFromKey()

	if got := extract("/pods/tenants/t1/default/nginx"); got != "t1" {
		t.Errorf("IdentityFromKey = %q, want %q", got, "t1")
	}
	if got := kl.KindRootPrefix("/pods"); got != "/pods/tenants/" {
		t.Errorf("KindRootPrefix = %q, want %q", got, "/pods/tenants/")
	}
}
