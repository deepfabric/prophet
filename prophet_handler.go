package prophet

import (
	"errors"
	"fmt"
	"time"
)

var (
	errReq                = errors.New("invalid req")
	errStaleResource      = errors.New("stale resource")
	errTombstoneContainer = errors.New("container is tombstone")
)

func (p *Prophet) handleResourceHeartbeat(msg *ResourceHeartbeatReq) (*resourceHeartbeatRsp, error) {
	if msg.LeaderPeer == nil && len(msg.Resource.Peers()) != 1 {
		return nil, errReq
	}

	if msg.Resource.ID() == 0 {
		return nil, errReq
	}

	value := newResourceRuntime(msg.Resource, msg.LeaderPeer)
	value.downPeers = msg.DownPeers
	value.pendingPeers = msg.PendingPeers

	p.Lock()
	defer p.Unlock()

	err := p.rt.handleResource(value)
	if err != nil {
		return nil, err
	}

	if len(value.meta.Peers()) == 0 {
		return nil, errReq
	}

	return p.coordinator.dispatch(p.rt.GetResource(value.meta.ID())), nil
}

func (p *Prophet) handleContainerHeartbeat(msg *ContainerHeartbeatReq) error {
	meta := msg.Container
	if meta != nil && meta.State() == Tombstone {
		return errTombstoneContainer
	}

	p.Lock()
	defer p.Unlock()

	container := p.rt.GetContainer(meta.ID())
	if container == nil {
		return fmt.Errorf("container %d not found", meta.ID())
	}

	container.busy = msg.Busy
	container.leaderCount = msg.LeaderCount
	container.replicaCount = msg.ReplicaCount
	container.storageCapacity = msg.StorageCapacity
	container.storageAvailable = msg.StorageAvailable
	container.sendingSnapCount = msg.SendingSnapCount
	container.receivingSnapCount = msg.ReceivingSnapCount
	container.applyingSnapCount = msg.ApplyingSnapCount
	container.lastHeartbeatTS = time.Now()

	p.rt.handleContainer(container)
	return nil
}
