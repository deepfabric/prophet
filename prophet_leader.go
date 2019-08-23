package prophet

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"
)

var (
	loopInterval = 200 * time.Millisecond
)

// Node is prophet info
type Node struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

func (n *Node) marshal() string {
	data, _ := json.Marshal(n)
	return string(data)
}

func (p *defaultProphet) startLeaderLoop() {
	p.runner.RunCancelableTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				log.Infof("prophet: exit the leader election loop")
				return
			default:
				log.Infof("prophet: ready to fetch leader")
				leader, rev, err := p.store.GetCurrentLeader()
				if err != nil {
					log.Errorf("prophet: get current leader failure, errors:\n %+v",
						err)
					time.Sleep(loopInterval)
					continue
				}
				log.Infof("prophet: fetch leader: %+v", leader)

				if leader != nil {
					if p.cfg.StorageNode && p.isMatchLeader(leader) {
						// oh, we are already leader, we may meet something wrong
						// in previous campaignLeader. we can resign and campaign again.
						log.Warnf("prophet: leader is matched, resign and campaign again, leader is <%v>",
							leader)
						if err = p.store.ResignLeader(); err != nil {
							log.Warnf("prophet: resign leader failure, leader <%v>, errors:\n %+v",
								leader,
								err)
							time.Sleep(loopInterval)
							continue
						}
					} else {
						log.Infof("prophet: we are not leader, watch the leader <%v>",
							leader)
						p.leader = leader // reset leader node for forward
						p.notifyElectionComplete()
						p.cfg.Handler.ProphetBecomeFollower()
						log.Infof("prophet: leader changed to %v", leader)
						p.store.WatchLeader(rev)
						log.Infof("prophet: leader %v out", leader)
					}
				}

				if p.cfg.StorageNode {
					log.Debugf("prophet: begin to campaign leader %s",
						p.node.Name)
					if err = p.store.CampaignLeader(p.cfg.LeaseTTL, p.enableLeader, p.disableLeader); err != nil {
						log.Errorf("prophet: campaign leader failure, errors:\n %+v", err)
					}
				}
			}
		}
	})
	<-p.completeC
}

func (p *defaultProphet) enableLeader() {
	log.Infof("prophet: ********become to leader now********")
	p.leader = p.node

	p.rt = newRuntime(p)
	p.rt.load()

	p.coordinator = newCoordinator(p.cfg, p.runner, p.rt)
	p.coordinator.start()

	p.wn = newWatcherNotifier(p.rt)
	go p.wn.start()

	// now, we are leader
	atomic.StoreInt64(&p.leaderFlag, 1)

	p.notifyElectionComplete()
	p.cfg.Handler.ProphetBecomeLeader()
}

func (p *defaultProphet) disableLeader() {
	atomic.StoreInt64(&p.leaderFlag, 0)
	log.Infof("prophet: ********become to follower now********")
	p.leader = nil

	// now, we are not leader
	if p.coordinator != nil {
		p.coordinator.stop()
		p.rt = nil
	}

	if p.wn != nil {
		p.wn.stop()
	}

	p.cfg.Handler.ProphetBecomeFollower()
}

func (p *defaultProphet) isLeader() bool {
	return 1 == atomic.LoadInt64(&p.leaderFlag)
}

func (p *defaultProphet) notifyElectionComplete() {
	if p.completeC != nil {
		p.completeC <- struct{}{}
	}
}

func (p *defaultProphet) isMatchLeader(leaderNode *Node) bool {
	return leaderNode != nil &&
		p.node.Name == leaderNode.Name
}
