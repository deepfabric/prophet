package prophet

import (
	"context"
	"time"

	"github.com/fagongzi/goetty"
)

// HeartbeatHandler handle for heartbeat rsp
type HeartbeatHandler interface {
	ChangeLeader(resourceID, newLeaderPeerID uint64)
	ChangePeer(resourceID, targetPeerID uint64, changeType ChangePeerType)
}

func (p *Prophet) startResourceHeartbeatLoop() {
	p.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(p.opts.resourceInterval)
		defer ticker.Stop()

		var conn goetty.IOSession
		for {
			select {
			case <-ctx.Done():
				if nil != conn {
					conn.Close()
				}
				return
			case <-ticker.C:
				if conn == nil {
					conn = p.getLeaderClient()
				}

				hbs := p.opts.resourceHBFetch()
				var err error
			OUTER:
				for _, hb := range hbs {
					for {
						err = conn.WriteAndFlush(hb)
						if err != nil {
							conn.Close()
							conn = nil
							log.Errorf("prophet: send resource heartbeat failed, errors: %+v", err)
							break OUTER
						}

						// read rsp
						msg, err := conn.Read()
						if err != nil {
							conn.Close()
							conn = nil
							log.Errorf("prophet: read heartbeat rsp failed, errors: %+v", err)
							break OUTER
						}

						if rsp, ok := msg.(*errorRsp); ok {
							conn.Close()
							conn = nil
							log.Infof("prophet: read heartbeat rsp with error %s", rsp.err)
						} else if rsp, ok := msg.(*resourceHeartbeatRsp); ok {
							if rsp.NewLeaderPeerID > 0 {
								p.opts.hbHandler.ChangeLeader(rsp.ResourceID, rsp.NewLeaderPeerID)
							} else if rsp.TargetPeerID > 0 {
								p.opts.hbHandler.ChangePeer(rsp.ResourceID, rsp.TargetPeerID, rsp.ChangeType)
							}
						}
					}
				}
			}
		}
	})
}

func (p *Prophet) startContainerHeartbeatLoop() {
	p.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(p.opts.containerInterval)
		defer ticker.Stop()

		var conn goetty.IOSession
		for {
			select {
			case <-ctx.Done():
				if nil != conn {
					conn.Close()
				}
				return
			case <-ticker.C:
				if conn == nil {
					conn = p.getLeaderClient()
				}

				req := p.opts.containerHBFetch()
				err := conn.WriteAndFlush(req)
				if err != nil {
					conn.Close()
					conn = nil
					log.Errorf("prophet: send container heartbeat failed, errors: %+v", err)
					continue
				}

				// read rsp
				msg, err := conn.Read()
				if err != nil {
					conn.Close()
					conn = nil
					log.Errorf("prophet: read container rsp failed, errors: %+v", err)
					continue
				}

				if rsp, ok := msg.(*errorRsp); ok {
					conn.Close()
					conn = nil
					log.Infof("prophet: read heartbeat rsp with error %s", rsp.err)
				}
			}
		}
	})
}
