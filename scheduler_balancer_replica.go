package prophet

type balanceReplicaScheduler struct {
	limit       uint64
	freezeCache *resourceFreezeCache
	selector    Selector
}

func newBalanceReplicaScheduler() Scheduler {
	freezeCache := newResourceFreezeCache(cfg.MaxFreezeScheduleInterval, 4*cfg.MaxFreezeScheduleInterval)

	var filters []Filter
	filters = append(filters, NewCacheFilter(freezeCache))
	filters = append(filters, NewStateFilter())
	filters = append(filters, NewHealthFilter())
	filters = append(filters, NewStorageThresholdFilter())
	filters = append(filters, NewSnapshotCountFilter())

	freezeCache.startGC()
	return &balanceReplicaScheduler{
		freezeCache: freezeCache,
		limit:       1,
		selector:    newBalanceSelector(ReplicaKind, filters),
	}
}

func (s *balanceReplicaScheduler) Name() string {
	return "balance-replica-scheduler"
}

func (s *balanceReplicaScheduler) ResourceKind() ResourceKind {
	return ReplicaKind
}

func (s *balanceReplicaScheduler) ResourceLimit() uint64 {
	return minUint64(s.limit, cfg.MaxRebalanceReplica)
}

func (s *balanceReplicaScheduler) Prepare(rt *Runtime) error { return nil }

func (s *balanceReplicaScheduler) Cleanup(rt *Runtime) {}

func (s *balanceReplicaScheduler) Schedule(rt *Runtime) Operator {
	// Select a peer from the container with most resources.
	res, oldPeer := scheduleRemovePeer(rt, s.selector)
	if res == nil {
		return nil
	}

	// We don't schedule resource with abnormal number of replicas.
	if len(res.meta.Peers()) != int(cfg.CountResourceReplicas) {
		return nil
	}

	op := s.transferPeer(rt, res, oldPeer)
	if op == nil {
		// We can't transfer peer from this container now, so we add it to the cache
		// and skip it for a while.
		s.freezeCache.set(oldPeer.ContainerID, nil)
	}

	return op
}

func (s *balanceReplicaScheduler) transferPeer(rt *Runtime, res *ResourceRuntime, oldPeer *Peer) Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	containers := rt.GetResourceContainers(res)
	source := rt.GetContainer(oldPeer.ContainerID)
	scoreGuard := NewDistinctScoreFilter(containers, source)

	checker := newReplicaChecker(rt)
	newPeer, _ := checker.selectBestPeer(res, true, scoreGuard)
	if newPeer == nil {
		return nil
	}

	target := rt.GetContainer(newPeer.ContainerID)
	if !shouldBalance(source, target, s.ResourceKind()) {
		return nil
	}

	s.limit = adjustBalanceLimit(rt, s.ResourceKind())
	return newTransferPeerAggregationOp(res, oldPeer, newPeer)
}

// scheduleRemovePeer schedules a resource to remove the peer.
func scheduleRemovePeer(rt *Runtime, s Selector, filters ...Filter) (*ResourceRuntime, *Peer) {
	containers := rt.GetContainers()

	source := s.SelectSource(containers, filters...)
	if source == nil {
		return nil, nil
	}

	target := rt.RandFollowerResource(source.meta.ID())
	if target == nil {
		target = rt.RandLeaderResource(source.meta.ID())
	}
	if target == nil {
		return nil, nil
	}

	return target, target.GetContainerPeer(source.meta.ID())
}
