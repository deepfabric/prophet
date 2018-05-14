package prophet

import (
	"context"
	"sync"
	"time"
)

// Coordinator resource coordinator
type Coordinator struct {
	sync.RWMutex

	rt         *Runtime
	checker    *replicaChecker
	limiter    *scheduleLimiter
	schedulers map[string]*scheduleController
	opts       map[uint64]Operator

	runner *Runner
}

func newCoordinator(runner *Runner, rt *Runtime) *Coordinator {
	c := new(Coordinator)
	c.limiter = newScheduleLimiter()
	c.checker = newReplicaChecker(rt)
	c.opts = make(map[uint64]Operator)
	c.schedulers = make(map[string]*scheduleController)
	c.runner = runner
	c.rt = rt
	return c
}

func (c *Coordinator) start() {
	for _, s := range cfg.schedulers {
		c.addScheduler(s)
	}
}

func (c *Coordinator) stop() {
	c.runner.Stop()
}

func (c *Coordinator) addScheduler(scheduler Scheduler) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.Name()]; ok {
		return ErrSchedulerExisted
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.rt); err != nil {
		return err
	}

	c.runner.RunCancelableTask(func(ctx context.Context) {
		c.runScheduler(ctx, s)
	})

	c.schedulers[s.Name()] = s
	return nil
}

func (c *Coordinator) addOperator(op Operator) bool {
	c.Lock()
	defer c.Unlock()

	id := op.ResourceID()
	if _, ok := c.opts[id]; ok {
		return false
	}

	c.limiter.addOperator(op)
	c.opts[id] = op
	return true
}

func (c *Coordinator) getOperator(id uint64) Operator {
	c.RLock()
	defer c.RUnlock()

	return c.opts[id]
}

func (c *Coordinator) removeOperator(op Operator) {
	c.Lock()
	defer c.Unlock()

	id := op.ResourceID()
	c.limiter.removeOperator(op)
	delete(c.opts, id)
}

func (c *Coordinator) runScheduler(ctx context.Context, s *scheduleController) {
	defer s.Cleanup(c.rt)

	timer := time.NewTimer(s.Interval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("prophet: scheduler %s stopped", s.Name())
			return
		case <-timer.C:
			timer.Reset(s.Interval())

			s.Lock()
			if !s.AllowSchedule() {
				s.Unlock()
				continue
			}

			for i := 0; i < cfg.MaxScheduleRetries; i++ {
				op := s.Schedule(c.rt)
				if op == nil {
					continue
				}
				if c.addOperator(op) {
					break
				}
			}
			s.Unlock()
		}
	}
}

// dispatch is used for coordinator resource,
// it will coordinator when the heartbeat arrives
func (c *Coordinator) dispatch(target *ResourceRuntime) *resourceHeartbeatRsp {
	// Check existed operator.
	if op := c.getOperator(target.meta.ID()); op != nil {
		res, finished := op.Do(target)
		if !finished {
			return res
		}
		c.removeOperator(op)
	}

	// Check replica operator.
	if c.limiter.operatorCount(ReplicaKind) >= cfg.MaxScheduleReplica {
		return nil
	}

	if op := c.checker.Check(target); op != nil {
		if c.addOperator(op) {
			res, _ := op.Do(target)
			return res
		}
	}

	return nil
}
