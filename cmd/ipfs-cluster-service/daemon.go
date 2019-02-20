package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	ipfscluster "github.com/ipfs/ipfs-cluster"
	"github.com/ipfs/ipfs-cluster/allocator/ascendalloc"
	"github.com/ipfs/ipfs-cluster/allocator/descendalloc"
	"github.com/ipfs/ipfs-cluster/api/ipfsproxy"
	"github.com/ipfs/ipfs-cluster/api/rest"
	"github.com/ipfs/ipfs-cluster/consensus/crdt"
	"github.com/ipfs/ipfs-cluster/consensus/raft"
	"github.com/ipfs/ipfs-cluster/informer/disk"
	"github.com/ipfs/ipfs-cluster/informer/numpin"
	"github.com/ipfs/ipfs-cluster/ipfsconn/ipfshttp"
	"github.com/ipfs/ipfs-cluster/monitor/basic"
	"github.com/ipfs/ipfs-cluster/monitor/pubsubmon"
	"github.com/ipfs/ipfs-cluster/observations"
	"github.com/ipfs/ipfs-cluster/pintracker/maptracker"
	"github.com/ipfs/ipfs-cluster/pintracker/stateless"
	"github.com/ipfs/ipfs-cluster/pstoremgr"

	ds "github.com/ipfs/go-datastore"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	ma "github.com/multiformats/go-multiaddr"
	cli "github.com/urfave/cli"
)

func parseBootstraps(flagVal []string) (bootstraps []ma.Multiaddr) {
	for _, a := range flagVal {
		bAddr, err := ma.NewMultiaddr(a)
		checkErr("error parsing bootstrap multiaddress (%s)", err, a)
		bootstraps = append(bootstraps, bAddr)
	}
	return
}

// Runs the cluster peer
func daemon(c *cli.Context) error {
	logger.Info("Initializing. For verbose output run with \"-l debug\". Please wait...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Deprecated
	// if c.Bool("upgrade") {}

	bootstraps := parseBootstraps(c.StringSlice("bootstrap"))

	// Execution lock
	locker.lock()
	defer locker.tryUnlock()

	// Load all the configurations
	cfgMgr, cfgs := makeAndLoadConfigs()
	defer cfgMgr.Shutdown()

	if c.Bool("stats") {
		cfgs.metricsCfg.EnableStats = true
	}

	cfgs = propagateTracingConfig(cfgs, c.Bool("tracing"))

	// Cleanup state if bootstrapping
	raftStaging := false
	if len(bootstraps) > 0 && c.String("consensus") == "raft" {
		raft.CleanupRaft(cfgs.raftCfg)
		raftStaging = true
	}

	if c.Bool("leave") {
		cfgs.clusterCfg.LeaveOnShutdown = true
	}

	cluster, err := createCluster(ctx, c, cfgs, raftStaging)
	checkErr("starting cluster", err)

	// noop if no bootstraps
	// if bootstrapping fails, consensus will never be ready
	// and timeout. So this can happen in background and we
	// avoid worrying about error handling here (since Cluster
	// will realize).
	go bootstrap(ctx, cluster, bootstraps)

	return handleSignals(ctx, cluster)
}

func createCluster(
	ctx context.Context,
	c *cli.Context,
	cfgs *cfgs,
	raftStaging bool,
) (*ipfscluster.Cluster, error) {
	host, err := ipfscluster.NewClusterHost(ctx, cfgs.clusterCfg)
	checkErr("creating libP2P Host", err)

	peerstoreMgr := pstoremgr.New(host, cfgs.clusterCfg.GetPeerstorePath())
	peerstoreMgr.ImportPeersFromPeerstore(false)

	api, err := rest.NewAPIWithHost(ctx, cfgs.apiCfg, host)
	checkErr("creating REST API component", err)

	proxy, err := ipfsproxy.New(cfgs.ipfsproxyCfg)
	checkErr("creating IPFS Proxy component", err)

	apis := []ipfscluster.API{api, proxy}

	connector, err := ipfshttp.NewConnector(cfgs.ipfshttpCfg)
	checkErr("creating IPFS Connector component", err)

	store := setupDatastore(c.String("consensus"), cfgs)

	cons := setupConsensus(
		c.String("consensus"),
		host,
		cfgs,
		store,
		raftStaging,
	)

	tracker := setupPinTracker(
		c.String("pintracker"),
		host,
		cfgs.maptrackerCfg,
		cfgs.statelessTrackerCfg,
		cfgs.clusterCfg.Peername,
	)

	var peersF func(context.Context) ([]peer.ID, error)
	if c.String("consensus") == "raft" {
		peersF = cons.Peers
	}

	mon := setupMonitor(
		c.String("monitor"),
		host,
		cfgs.monCfg,
		cfgs.pubsubmonCfg,
		peersF,
	)

	informer, alloc := setupAllocation(
		c.String("alloc"),
		cfgs.diskInfCfg,
		cfgs.numpinInfCfg,
	)

	ipfscluster.ReadyTimeout = cfgs.raftCfg.WaitForLeaderTimeout + 5*time.Second

	err = observations.SetupMetrics(cfgs.metricsCfg)
	checkErr("setting up Metrics", err)

	tracer, err := observations.SetupTracing(cfgs.tracingCfg)
	checkErr("setting up Tracing", err)

	return ipfscluster.NewCluster(
		host,
		cfgs.clusterCfg,
		cons,
		apis,
		connector,
		tracker,
		mon,
		alloc,
		informer,
		tracer,
	)
}

// bootstrap will bootstrap this peer to one of the bootstrap addresses
// if there are any.
func bootstrap(ctx context.Context, cluster *ipfscluster.Cluster, bootstraps []ma.Multiaddr) {
	for _, bstrap := range bootstraps {
		logger.Infof("Bootstrapping to %s", bstrap)
		err := cluster.Join(ctx, bstrap)
		if err != nil {
			logger.Errorf("bootstrap to %s failed: %s", bstrap, err)
		}
	}
}

func handleSignals(ctx context.Context, cluster *ipfscluster.Cluster) error {
	signalChan := make(chan os.Signal, 20)
	signal.Notify(
		signalChan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGHUP,
	)

	var ctrlcCount int
	for {
		select {
		case <-signalChan:
			ctrlcCount++
			handleCtrlC(ctx, cluster, ctrlcCount)
		case <-cluster.Done():
			return nil
		}
	}
}

func handleCtrlC(ctx context.Context, cluster *ipfscluster.Cluster, ctrlcCount int) {
	switch ctrlcCount {
	case 1:
		go func() {
			err := cluster.Shutdown(ctx)
			checkErr("shutting down cluster", err)
		}()
	case 2:
		out(`


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Shutdown is taking too long! Press Ctrl-c again to manually kill cluster.
Note that this may corrupt the local cluster state.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


`)
	case 3:
		out("exiting cluster NOW")
		locker.tryUnlock()
		os.Exit(-1)
	}
}

func setupAllocation(
	name string,
	diskInfCfg *disk.Config,
	numpinInfCfg *numpin.Config,
) (ipfscluster.Informer, ipfscluster.PinAllocator) {
	switch name {
	case "disk", "disk-freespace":
		informer, err := disk.NewInformer(diskInfCfg)
		checkErr("creating informer", err)
		return informer, descendalloc.NewAllocator()
	case "disk-reposize":
		informer, err := disk.NewInformer(diskInfCfg)
		checkErr("creating informer", err)
		return informer, ascendalloc.NewAllocator()
	case "numpin", "pincount":
		informer, err := numpin.NewInformer(numpinInfCfg)
		checkErr("creating informer", err)
		return informer, ascendalloc.NewAllocator()
	default:
		err := errors.New("unknown allocation strategy")
		checkErr("", err)
		return nil, nil
	}
}

func setupMonitor(
	name string,
	h host.Host,
	basicCfg *basic.Config,
	pubsubCfg *pubsubmon.Config,
	peers func(ctx context.Context) ([]peer.ID, error),
) ipfscluster.PeerMonitor {
	switch name {
	case "basic":
		mon, err := basic.NewMonitor(basicCfg, peers)
		checkErr("creating monitor", err)
		logger.Debug("basic monitor loaded")
		return mon
	case "pubsub":
		mon, err := pubsubmon.New(h, pubsubCfg, peers)
		checkErr("creating monitor", err)
		logger.Debug("pubsub monitor loaded")
		return mon
	default:
		err := errors.New("unknown monitor type")
		checkErr("", err)
		return nil
	}

}

func setupPinTracker(
	name string,
	h host.Host,
	mapCfg *maptracker.Config,
	statelessCfg *stateless.Config,
	peerName string,
) ipfscluster.PinTracker {
	switch name {
	case "map":
		ptrk := maptracker.NewMapPinTracker(mapCfg, h.ID(), peerName)
		logger.Debug("map pintracker loaded")
		return ptrk
	case "stateless":
		ptrk := stateless.New(statelessCfg, h.ID(), peerName)
		logger.Debug("stateless pintracker loaded")
		return ptrk
	default:
		err := errors.New("unknown pintracker type")
		checkErr("", err)
		return nil
	}
}

func setupDatastore(
	consensus string,
	cfgs *cfgs,
) ds.ThreadSafeDatastore {
	stmgr := newStateManager(consensus, cfgs)
	store, err := stmgr.GetStore()
	checkErr("creating datastore", err)
	return store
}

func setupConsensus(
	name string,
	h host.Host,
	cfgs *cfgs,
	store ds.ThreadSafeDatastore,
	raftStaging bool,
) ipfscluster.Consensus {
	switch name {
	case "raft":
		rft, err := raft.NewConsensus(
			h,
			cfgs.raftCfg,
			store,
			raftStaging,
		)
		checkErr("creating Raft component", err)
		return rft
	case "crdt":
		convrdt, err := crdt.New(
			h,
			cfgs.crdtCfg,
			store,
		)
		checkErr("creating CRDT component", err)
		return convrdt
	default:
		err := errors.New("unknown consensus component")
		checkErr("", err)
		return nil
	}
}
