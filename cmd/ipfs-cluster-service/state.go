package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	ipfscluster "github.com/ipfs/ipfs-cluster"
	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/consensus/crdt"
	"github.com/ipfs/ipfs-cluster/consensus/raft"
	"github.com/ipfs/ipfs-cluster/datastore/badger"
	"github.com/ipfs/ipfs-cluster/datastore/inmem"
	"github.com/ipfs/ipfs-cluster/ipfsconn/ipfshttp"
	"github.com/ipfs/ipfs-cluster/pstoremgr"
	"github.com/ipfs/ipfs-cluster/state"

	ds "github.com/ipfs/go-datastore"
)

type stateManager interface {
	ImportState(io.Reader) error
	ExportState(io.Writer) error
	GetStore() (ds.ThreadSafeDatastore, error)
	Clean() error
}

func newStateManager(consensus string, cfgs *cfgs) stateManager {
	switch consensus {
	case "raft":
		return &raftStateManager{cfgs}
	case "crdt":
		return &crdtStateManager{cfgs}
	case "":
		checkErr("", errors.New("unspecified consensus component"))
	default:
		checkErr("", fmt.Errorf("unknown consensus component '%s'", consensus))
	}
	return nil
}

type raftStateManager struct {
	cfgs *cfgs
}

func (raftsm *raftStateManager) GetStore() (ds.ThreadSafeDatastore, error) {
	return (inmem.New()).(ds.ThreadSafeDatastore), nil
}

func (raftsm *raftStateManager) getOfflineState() (state.State, error) {
	store, err := raftsm.GetStore()
	if err != nil {
		return nil, err
	}
	return raft.OfflineState(raftsm.cfgs.raftCfg, store)
}

func (raftsm *raftStateManager) ImportState(r io.Reader) error {
	err := raftsm.Clean()
	if err != nil {
		return err
	}

	st, err := raftsm.getOfflineState()
	if err != nil {
		return err
	}
	err = importState(r, st)
	if err != nil {
		return err
	}
	pm := pstoremgr.New(nil, raftsm.cfgs.clusterCfg.GetPeerstorePath())
	raftPeers := append(
		ipfscluster.PeersFromMultiaddrs(pm.LoadPeerstore()),
		raftsm.cfgs.clusterCfg.ID,
	)
	return raft.SnapshotSave(raftsm.cfgs.raftCfg, st, raftPeers)
}

func (raftsm *raftStateManager) ExportState(w io.Writer) error {
	st, err := raftsm.getOfflineState()
	if err != nil {
		return err
	}
	return exportState(w, st)
}

func (raftsm *raftStateManager) Clean() error {
	return raft.CleanupRaft(raftsm.cfgs.raftCfg)
}

type crdtStateManager struct {
	cfgs *cfgs
}

func (crdtsm *crdtStateManager) GetStore() (ds.ThreadSafeDatastore, error) {
	bds, err := badger.New(crdtsm.cfgs.badgerCfg)
	if err != nil {
		return nil, err
	}
	return bds.(ds.ThreadSafeDatastore), nil
}

func (crdtsm *crdtStateManager) getOfflineState(ipfs ipfscluster.IPFSConnector) (state.BatchingState, error) {
	store, err := crdtsm.GetStore()
	if err != nil {
		return nil, err
	}
	return crdt.OfflineState(crdtsm.cfgs.crdtCfg, store, ipfs)
}

func (crdtsm *crdtStateManager) ImportState(r io.Reader) error {
	ipfs, err := ipfshttp.NewConnector(crdtsm.cfgs.ipfshttpCfg)
	if err != nil {
		return err
	}

	err = crdtsm.Clean()
	if err != nil {
		return err
	}

	st, err := crdtsm.getOfflineState(ipfs)
	if err != nil {
		return err
	}

	err = importState(r, st)
	if err != nil {
		return err
	}

	return st.Commit(context.Background())
}

func (crdtsm *crdtStateManager) ExportState(w io.Writer) error {
	st, err := crdtsm.getOfflineState(nil)
	if err != nil {
		return err
	}
	return exportState(w, st)
}

func (crdtsm *crdtStateManager) Clean() error {
	return badger.Cleanup(crdtsm.cfgs.badgerCfg)
}

func importState(r io.Reader, st state.State) error {
	ctx := context.Background()
	dec := json.NewDecoder(r)
	for {
		var pin api.Pin
		err := dec.Decode(&pin)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = st.Add(ctx, &pin)
		if err != nil {
			return err
		}
	}
}

// ExportState saves a json representation of a state
func exportState(w io.Writer, st state.State) error {
	pins, err := st.List(context.Background())
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	for _, pin := range pins {
		err := enc.Encode(pin)
		if err != nil {
			return err
		}
	}
	return nil
}
