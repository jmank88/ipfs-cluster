package crdt

import (
	"context"

	ipfscluster "github.com/ipfs/ipfs-cluster"
	"github.com/ipfs/ipfs-cluster/api"

	blockfmt "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	rpc "github.com/libp2p/go-libp2p-gorpc"
)

// A DAGSyncer component implementation. Guess what, it uses IPFS.
type rpcDAGSyncer struct {
	rpcClient *rpc.Client
}

func (ibs *rpcDAGSyncer) Get(ctx context.Context, c cid.Cid) (blockfmt.Block, error) {
	var blockBytes []byte

	err := ibs.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"IPFSBlockGet",
		api.PinCid(c).ToSerial(),
		&blockBytes,
	)
	if err != nil {
		return nil, err
	}

	return blockfmt.NewBlockWithCid(blockBytes, c)
}

func (ibs *rpcDAGSyncer) Put(ctx context.Context, b blockfmt.Block) error {
	data := b.RawData()
	nwm := api.NodeWithMeta{
		Data:   data,
		Format: "protobuf",
	}

	return ibs.rpcClient.CallContext(
		ctx,
		"",
		"Cluster",
		"IPFSBlockPut",
		nwm,
		&struct{}{},
	)
}

type ipfsConnDAGSyncer struct {
	ipfs ipfscluster.IPFSConnector
}

func (ipfsc *ipfsConnDAGSyncer) Get(ctx context.Context, c cid.Cid) (blockfmt.Block, error) {
	blockBytes, err := ipfsc.ipfs.BlockGet(ctx, c)
	if err != nil {
		return nil, err
	}

	return blockfmt.NewBlockWithCid(blockBytes, c)
}

func (ipfsc *ipfsConnDAGSyncer) Put(ctx context.Context, b blockfmt.Block) error {
	data := b.RawData()
	nwm := api.NodeWithMeta{
		Data:   data,
		Format: "protobuf",
	}
	return ipfsc.ipfs.BlockPut(ctx, nwm)
}
