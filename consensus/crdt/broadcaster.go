package crdt

import (
	"context"

	peer "github.com/libp2p/go-libp2p-peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type pubsubBroadcaster struct {
	ctx context.Context

	psub *pubsub.PubSub
	subs *pubsub.Subscription

	trustedFunc func(p peer.ID) bool
}

func (pbc *pubsubBroadcaster) Broadcast(data []byte) error {
	return pbc.psub.Publish(pbc.subs.Topic(), data)
}

func (pbc *pubsubBroadcaster) Next() ([]byte, error) {
	var msg *pubsub.Message
	var err error
	for {
		select {
		case <-pbc.ctx.Done():
			return nil, pbc.ctx.Err()
		default:
		}

		msg, err = pbc.subs.Next(pbc.ctx)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		if !pbc.trustedFunc(msg.GetFrom()) {
			logger.Debug("ignoring message from untrusted peer ")
			continue
		}

		break
	}
	return msg.GetData(), nil
}
