package ipfscluster

import (
	"context"
	"encoding/hex"

	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-host"
	ipnet "github.com/libp2p/go-libp2p-interface-pnet"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pnet "github.com/libp2p/go-libp2p-pnet"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	routedhost "github.com/libp2p/go-libp2p/p2p/host/routed"
	ma "github.com/multiformats/go-multiaddr"
)

// NewClusterHost creates a libp2p Host with the options from the provided
// cluster configuration. Using that host, it creates pubsub and a DHT
// instances, for shared used by all cluster components. The returned host uses
// the DHT for routing.
func NewClusterHost(
	ctx context.Context,
	cfg *Config,
) (host.Host, *pubsub.PubSub, *dht.IpfsDHT, error) {
	var prot ipnet.Protector
	var err error

	// Create protector if we have a secret.
	if cfg.Secret != nil && len(cfg.Secret) > 0 {
		var key [32]byte
		copy(key[:], cfg.Secret)
		prot, err = pnet.NewV1ProtectorFromBytes(&key)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	h, err := libp2p.New(
		ctx,
		libp2p.Identity(cfg.PrivateKey),
		libp2p.ListenAddrs([]ma.Multiaddr{cfg.ListenAddr}...),
		libp2p.PrivateNetwork(prot),
		libp2p.NATPortMap(),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	psub, err := pubsub.NewGossipSub(
		ctx,
		h,
		pubsub.WithMessageSigning(true),
		pubsub.WithStrictSignatureVerification(true),
	)
	if err != nil {
		h.Close()
		return nil, nil, nil, err
	}

	idht, err := dht.New(ctx, h)
	if err != nil {
		h.Close()
		return nil, nil, nil, err
	}
	err = idht.Bootstrap(ctx)
	if err != nil {
		h.Close()
		return nil, nil, nil, err
	}

	rHost := routedhost.Wrap(h, idht)
	return rHost, psub, idht, nil
}

// EncodeProtectorKey converts a byte slice to its hex string representation.
func EncodeProtectorKey(secretBytes []byte) string {
	return hex.EncodeToString(secretBytes)
}
