// Package inmem provides a in-memory datastore for use with Cluster.
package inmem

import (
	"github.com/ipfs/ipfs-cluster/state/dsstate"

	ds "github.com/ipfs/go-datastore"
	sync "github.com/ipfs/go-datastore/sync"
)

// MapState is mostly a MapDatastore to store the pinset.
type MapState struct {
	dst *dsstate.State
}

// New returns a new thread-safe in-memory go-datastore.
func New() ds.Datastore {
	mapDs := ds.NewMapDatastore()
	return sync.MutexWrap(mapDs)
}
