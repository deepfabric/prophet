package prophet

func (rc *Runtime) handleContainer(source *ContainerRuntime) {
	rc.Lock()
	defer rc.Unlock()

	rc.containers[source.meta.ID()] = source
}

func (rc *Runtime) handleResource(source *ResourceRuntime) error {
	rc.Lock()
	defer rc.Unlock()

	current := rc.getResourceWithoutLock(source.meta.ID())
	if current == nil {
		return rc.doPutResource(source)
	}

	// resource meta is stale, return an error.
	if current.meta.IsStale(source.meta) {
		return errStaleResource
	}

	rangeChanged := current.meta.RangeChanged(source.meta)
	peersChanged := current.meta.PeerChanged(source.meta)

	// resource meta is updated, update kv and cache.
	if rangeChanged || peersChanged {
		return rc.doPutResource(source)
	}

	if current.leaderPeer != nil &&
		current.leaderPeer.ID != source.leaderPeer.ID {
		log.Infof("prophet: resource %d leader changed, from %d to %d",
			current.meta.ID(),
			current.leaderPeer.ID,
			source.leaderPeer.ID)
	}

	// resource meta is the same, update cache only.
	rc.putResourceInCache(source)
	return nil
}

func (rc *Runtime) doPutResource(source *ResourceRuntime) error {
	err := rc.store.PutResource(source.meta)
	if err != nil {
		return err
	}

	rc.putResourceInCache(source)
	return nil
}

func (rc *Runtime) putResourceInCache(origin *ResourceRuntime) {
	if origin, ok := rc.resources[origin.meta.ID()]; ok {
		rc.removeResource(origin)
	}

	rc.resources[origin.meta.ID()] = origin

	if origin.leaderPeer == nil || origin.leaderPeer.ID == 0 {
		return
	}

	// Add to leaders and followers.
	for _, peer := range origin.meta.Peers() {
		containerID := peer.ContainerID
		if peer.ID == origin.leaderPeer.ID {
			// Add leader peer to leaders.
			container, ok := rc.leaders[containerID]
			if !ok {
				container = make(map[uint64]*ResourceRuntime)
				rc.leaders[containerID] = container
			}
			container[origin.meta.ID()] = origin
		} else {
			// Add follower peer to followers.
			container, ok := rc.followers[containerID]
			if !ok {
				container = make(map[uint64]*ResourceRuntime)
				rc.followers[containerID] = container
			}
			container[origin.meta.ID()] = origin
		}
	}
}

func (rc *Runtime) removeResource(origin *ResourceRuntime) {
	delete(rc.resources, origin.meta.ID())

	// Remove from leaders and followers.
	for _, peer := range origin.meta.Peers() {
		delete(rc.leaders[peer.ContainerID], origin.meta.ID())
		delete(rc.followers[peer.ContainerID], origin.meta.ID())
	}
}
