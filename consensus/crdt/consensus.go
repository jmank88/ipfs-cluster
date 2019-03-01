package crdt

import (
	"context"
	"errors"
	"sync"

	ipfscluster "github.com/ipfs/ipfs-cluster"
	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/state"
	"github.com/ipfs/ipfs-cluster/state/dsstate"

	ds "github.com/ipfs/go-datastore"
	crdt "github.com/ipfs/go-ds-crdt"
	logging "github.com/ipfs/go-log"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

var logger = logging.Logger("crdt")

// Common variables for the module.
var (
	ErrNoLeader = errors.New("crdt consensus component does not provide a leader")
)

// Consensus implement ipfscluster.Consensus and provides the facility to add
// and remove pins from the Cluster shared state. It uses a CRDT-backed
// implementation of go-datastore (go-ds-crdt).
type Consensus struct {
	ctx    context.Context
	cancel context.CancelFunc

	config *Config

	host host.Host

	store     ds.Datastore
	namespace ds.Key

	state state.State
	crdt  *crdt.Datastore

	pubsub       *pubsub.PubSub
	subscription *pubsub.Subscription

	rpcClient *rpc.Client
	rpcReady  chan struct{}
	readyCh   chan struct{}

	shutdownLock sync.RWMutex
	shutdown     bool
}

// New creates a new crdt Consensus component. The given PubSub will be used to
// broadcast new heads. The given thread-safe datastore will be used to persist
// data and all will be prefixed with cfg.DatastoreNamespace.
func New(
	host host.Host,
	pubsub *pubsub.PubSub,
	cfg *Config,
	store ds.ThreadSafeDatastore,
) (*Consensus, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	css := &Consensus{
		ctx:       ctx,
		cancel:    cancel,
		config:    cfg,
		host:      host,
		store:     store,
		namespace: ds.NewKey(cfg.DatastoreNamespace),
		pubsub:    pubsub,
		rpcReady:  make(chan struct{}, 1),
		readyCh:   make(chan struct{}, 1),
	}

	go css.setup()
	return css, nil
}

func (css *Consensus) setup() {
	select {
	case <-css.ctx.Done():
		return
	case <-css.rpcReady:
	}

	topicSub, err := css.pubsub.Subscribe(css.config.ClusterName)
	if err != nil {
		logger.Errorf("error subscribing to topic: %s", err)
		return
	}
	css.subscription = topicSub

	dagSyncer := &rpcDAGSyncer{
		css.rpcClient,
	}

	broadcaster := &pubsubBroadcaster{
		ctx:         css.ctx,
		psub:        css.pubsub,
		subs:        css.subscription,
		trustedFunc: func(p peer.ID) bool { return true },
	}

	opts := crdt.DefaultOptions()
	opts.Logger = logger

	crdt := crdt.New(
		css.store,
		css.namespace,
		dagSyncer,
		broadcaster,
		opts,
	)

	css.crdt = crdt

	clusterState, err := dsstate.New(
		css.crdt,
		// unsure if we should set something else but crdt is already
		// namespaced and this would only namespace the keys, which only
		// complicates things.
		"",
		dsstate.DefaultHandle(),
	)
	if err != nil {
		logger.Errorf("error creating cluster state datastore: %s", err)
		return
	}
	css.state = clusterState
	css.readyCh <- struct{}{}
}

// Shutdown closes this component, cancelling the pubsub subscription.
func (css *Consensus) Shutdown(ctx context.Context) error {
	css.shutdownLock.Lock()
	defer css.shutdownLock.Unlock()

	if css.shutdown {
		logger.Debug("already shutdown")
		return nil
	}

	logger.Info("stopping Consensus component")

	if sub := css.subscription; sub != nil {
		sub.Cancel()
	}

	if crdt := css.crdt; crdt != nil {
		crdt.Close()
	}

	if store := css.store; store != nil {
		// This might break things if more than
		// one component use the same datastore.
		// For the moment we assume it under our
		// full control.
		//
		// Also, all datastores are io.Closer now, but this has not
		// yet been bubbled.
		store.Close()
	}

	if css.config.hostShutdown {
		css.host.Close()
	}

	css.shutdown = true
	css.cancel()
	close(css.rpcReady)
	return nil
}

// SetClient gives the component the ability to communicate and
// leaves it ready to use.
func (css *Consensus) SetClient(c *rpc.Client) {
	css.rpcClient = c
	css.rpcReady <- struct{}{}
}

// Ready returns a channel which is signalled when the component
// is ready to use.
func (css *Consensus) Ready(ctx context.Context) <-chan struct{} {
	return css.readyCh
}

// LogPin adds a new pin to the shared state.
func (css *Consensus) LogPin(ctx context.Context, pin *api.Pin) error {
	err := css.state.Add(ctx, pin)
	if err != nil {
		return err
	}

	return css.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"Track",
		pin,
		&struct{}{},
	)
}

// LogUnpin removes a pin from the shared state.
func (css *Consensus) LogUnpin(ctx context.Context, pin *api.Pin) error {
	err := css.state.Rm(ctx, pin.Cid)
	if err != nil {
		return err
	}

	return css.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"Untrack",
		pin,
		&struct{}{},
	)
}

// Peers returns the current known peerset. It uses
// the monitor component and considers every peer with
// valid known metrics a memeber.
func (css *Consensus) Peers(ctx context.Context) ([]peer.ID, error) {
	var metrics []*api.Metric

	err := css.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"PeerMonitorLatestMetrics",
		css.config.PeersetMetric,
		&metrics,
	)
	if err != nil {
		return nil, err
	}

	peers := make([]peer.ID, len(metrics), len(metrics))

	for i, m := range metrics {
		peers[i] = m.Peer
	}
	return peers, nil
}

// WaitForSync is a no-op as it is not necessary to be fully synced for the
// component to be usable.
func (css *Consensus) WaitForSync(ctx context.Context) error { return nil }

// AddPeer is a no-op as we do not need to do peerset management with Merkle-CRDTs.
func (css *Consensus) AddPeer(ctx context.Context, pid peer.ID) error { return nil }

// RmPeer is a no-op.
func (css *Consensus) RmPeer(ctx context.Context, pid peer.ID) error { return nil }

// State returns the cluster shared state.
func (css *Consensus) State(ctx context.Context) (state.State, error) { return css.state, nil }

// Clean is a no-op. FIXME: It should delete all keys under namespace.
func (css *Consensus) Clean(context.Context) error { return nil }

// Leader returns ErrNoLeader.
func (css *Consensus) Leader(ctx context.Context) (peer.ID, error) {
	return nil, ErrNoLeader
}

// OfflineState returns an offline, read-only batching state using the given
// datastore. Any writes to this state are processed through the given
// ipfs connector (the state is offline as it does not require a
// running cluster peer).
func OfflineState(cfg *Config, store ds.ThreadSafeDatastore, ipfs ipfscluster.IPFSConnector) (state.BatchingState, error) {
	opts := crdt.DefaultOptions()
	opts.Logger = logger

	var dags crdt.DAGSyncer
	if ipfs != nil {
		dags = &ipfsConnDAGSyncer{
			ipfs,
		}
	}

	crdt := crdt.New(
		store,
		ds.NewKey(cfg.DatastoreNamespace),
		dags,
		nil,
		opts,
	)
	return dsstate.NewBatching(crdt, "", dsstate.DefaultHandle())
}
