// Package inmem provides a in-memory datastore for use with Cluster.
package inmem

import (
	ds "github.com/ipfs/go-datastore"
	sync "github.com/ipfs/go-datastore/sync"
)

// New returns a new thread-safe in-memory go-datastore.
func New() ds.Datastore {
	mapDs := ds.NewMapDatastore()
	return sync.MutexWrap(mapDs)
}
