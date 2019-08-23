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

func (p *defaultProphet) handleResourceHeartbeat(msg *ResourceHeartbeatReq) (*resourceHeartbeatRsp, error) {
	if msg.LeaderPeer == nil && len(msg.Resource.Peers()) != 1 {
		return nil, errReq
	}

	if msg.Resource.ID() == 0 {
		return nil, errReq
	}

	if len(msg.Resource.Peers()) == 0 {
		return nil, errReq
	}

	value := newResourceRuntime(msg.Resource, msg.LeaderPeer)
	value.downPeers = msg.DownPeers
	value.pendingPeers = msg.PendingPeers

	p.Lock()
	defer p.Unlock()

	err := p.rt.handleResource(value)
	if err != nil {
		if err == errStaleResource {
			return newChangePeerRsp(msg.Resource.ID(),
				value.GetContainerPeer(msg.ContainerID),
				RemovePeer), nil
		}

		return nil, err
	}

	return p.coordinator.dispatch(p.rt.Resource(value.meta.ID())), nil
}

func (p *defaultProphet) handleContainerHeartbeat(msg *ContainerHeartbeatReq) error {
	meta := msg.Container
	if meta != nil && meta.State() == Tombstone {
		return errTombstoneContainer
	}

	p.Lock()
	defer p.Unlock()

	container := p.rt.Container(meta.ID())
	if container == nil {
		err := p.store.PutContainer(meta)
		if err != nil {
			return err
		}

		container = newContainerRuntime(meta)
	}

	container.blocked = msg.Block
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

func (p *defaultProphet) handleAllocID(req *allocIDReq) *allocIDRsp {
	id, err := p.store.AllocID()
	return &allocIDRsp{
		ID:  id,
		Err: err,
	}
}

func (p *defaultProphet) handleAskSplit(req *askSplitReq) *askSplitRsp {
	p.Lock()
	defer p.Unlock()

	rsp := &askSplitRsp{}

	res := p.rt.Resource(req.Resource.ID())
	if res == nil {
		rsp.Err = fmt.Errorf("resource not found")
		return rsp
	}

	newID, err := p.store.AllocID()
	if err != nil {
		rsp.Err = err
		return rsp
	}

	cnt := len(req.Resource.Peers())
	peerIDs := make([]uint64, cnt)
	for index := 0; index < cnt; index++ {
		if peerIDs[index], err = p.store.AllocID(); err != nil {
			rsp.Err = err
			return rsp
		}
	}

	rsp.NewID = newID
	rsp.NewPeerIDs = peerIDs
	return rsp
}
