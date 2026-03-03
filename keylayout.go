package storage

import (
	"strings"
)

// KeyLayout describes how cluster identity is embedded in etcd key paths.
// The standard kplane layout is:
//
//	/<etcdPrefix>/<resourcePrefix>/clusters/<clusterID>/[<namespace>/]<name>
//
// This allows a single recursive watch on /<resourcePrefix>/clusters/ to
// capture events for all clusters of a resource type.
type KeyLayout struct {
	// ClusterSegment is the path segment that separates the resource prefix
	// from the cluster ID. Default: "clusters".
	ClusterSegment string
}

// DefaultKeyLayout returns a KeyLayout with the standard "clusters" segment.
func DefaultKeyLayout() KeyLayout {
	return KeyLayout{ClusterSegment: "clusters"}
}

// IdentityFromKey returns a function that extracts cluster ID from a storage key.
// The function looks for the cluster segment marker in the key and returns the
// next path component as the cluster ID.
//
// Example with ClusterSegment="clusters":
//
//	"/pods/clusters/c1/default/nginx" → "c1"
//	"/pods/default/nginx"             → ""
func (kl KeyLayout) IdentityFromKey() func(key string) string {
	marker := "/" + kl.ClusterSegment + "/"
	return func(key string) string {
		idx := strings.Index(key, marker)
		if idx < 0 {
			return ""
		}
		rest := key[idx+len(marker):]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			return rest[:slash]
		}
		return rest
	}
}

// ClusterFromKey extracts the cluster ID from a single key using this layout.
func (kl KeyLayout) ClusterFromKey(key string) string {
	return kl.IdentityFromKey()(key)
}

// KindRootPrefix returns the prefix that covers all clusters for a resource.
// This is the prefix used for shared watches.
//
// Example: KindRootPrefix("/pods") → "/pods/clusters/"
func (kl KeyLayout) KindRootPrefix(resourcePrefix string) string {
	rp := strings.TrimSuffix(resourcePrefix, "/")
	return rp + "/" + kl.ClusterSegment + "/"
}

// PerClusterPrefix returns the prefix for a specific cluster's objects.
//
// Example: PerClusterPrefix("/pods", "c1") → "/pods/clusters/c1/"
func (kl KeyLayout) PerClusterPrefix(resourcePrefix string, clusterID string) string {
	rp := strings.TrimSuffix(resourcePrefix, "/")
	return rp + "/" + kl.ClusterSegment + "/" + clusterID + "/"
}
