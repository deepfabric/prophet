package prophet

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
)

type electorTester struct {
	sync.Mutex

	group     uint64
	id        string
	leader    bool
	elector   Elector
	cancel    context.CancelFunc
	ctx       context.Context
	startedC  chan interface{}
	once      sync.Once
	blockTime time.Duration
}

func newElectorTester(group uint64, id string, elector Elector) *electorTester {
	ctx, cancel := context.WithCancel(context.Background())

	return &electorTester{
		startedC: make(chan interface{}),
		group:    group,
		id:       id,
		elector:  elector,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (t *electorTester) start() {
	go t.elector.ElectionLoop(t.ctx, t.group, t.id, t.becomeLeader, t.becomeFollower)
	<-t.startedC
}

func (t *electorTester) stop(blockTime time.Duration) {
	t.blockTime = blockTime
	t.cancel()
}

func (t *electorTester) isLeader() bool {
	t.Lock()
	defer t.Unlock()

	return t.leader
}

func (t *electorTester) becomeLeader() {
	t.Lock()
	defer t.Unlock()

	t.leader = true
	t.notifyStarted()

	if t.blockTime > 0 {
		<-time.After(t.blockTime)
	}
}

func (t *electorTester) becomeFollower() {
	t.Lock()
	defer t.Unlock()

	t.leader = false
	t.notifyStarted()

	if t.blockTime > 0 {
		<-time.After(t.blockTime)
	}
}

func (t *electorTester) notifyStarted() {
	t.once.Do(func() {
		if t.startedC != nil {
			close(t.startedC)
			t.startedC = nil
		}
	})
}

func TestElectionLoop(t *testing.T) {
	stopC, port, err := startTestSingleEtcd(t)
	if err != nil {
		assert.FailNowf(t, "start embed etcd failed", "error: %+v", err)
	}
	defer close(stopC)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(fmt.Sprintf("http://127.0.0.1:%d", port), ","),
		DialTimeout: DefaultTimeout,
	})
	assert.Nil(t, err, "create etcd client failed")
	defer client.Close()

	elector, err := NewElector(client, WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")
	defer elector.Stop(0)

	value1 := newElectorTester(0, "1", elector)
	value2 := newElectorTester(0, "2", elector)

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	assert.True(t, value1.isLeader(), "value1 must be leader")
	assert.False(t, value2.isLeader(), "value2 must be follower")

	value3 := newElectorTester(0, "", elector)
	value3.start()
	defer value3.stop(0)

	assert.False(t, value3.isLeader(), "value3 must be follower")

	value1.stop(0)
	time.Sleep(time.Second + time.Millisecond*200)
	assert.True(t, value2.isLeader(), "value2 must be leader")

	value2.stop(0)
	time.Sleep(time.Second + time.Millisecond*200)
	assert.False(t, value2.isLeader(), "value2 must be follower")
	assert.False(t, value3.isLeader(), "value3 must be follower")
}

func TestElectionLoopWithDistributedLock(t *testing.T) {
	stopC, port, err := startTestSingleEtcd(t)
	if err != nil {
		assert.FailNowf(t, "start embed etcd failed", "error: %+v", err)
	}
	defer close(stopC)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(fmt.Sprintf("http://127.0.0.1:%d", port), ","),
		DialTimeout: DefaultTimeout,
	})
	assert.Nil(t, err, "create etcd client failed")
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1),
		WithLockIfBecomeLeader(true))
	assert.Nil(t, err, "create elector failed")
	defer elector.Stop(0)

	value1 := newElectorTester(0, "1", elector)
	value2 := newElectorTester(0, "2", elector)

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	assert.True(t, value1.isLeader(), "value1 must be leader")
	assert.False(t, value2.isLeader(), "value2 must be follower")

	value1.stop(time.Second * 2)
	time.Sleep(time.Second + time.Millisecond*200)
	assert.False(t, value2.isLeader(), "value2 must be follower before distributed lock released")

	time.Sleep(time.Second + time.Millisecond*200)
	assert.True(t, value2.isLeader(), "value2 must be leader after distributed lock released")
}

func TestChangeLeaderTo(t *testing.T) {
	stopC, port, err := startTestSingleEtcd(t)
	if err != nil {
		assert.FailNowf(t, "start embed etcd failed", "error: %+v", err)
	}
	defer close(stopC)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(fmt.Sprintf("http://127.0.0.1:%d", port), ","),
		DialTimeout: DefaultTimeout,
	})
	assert.Nil(t, err, "create etcd client failed")
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")
	defer elector.Stop(0)

	value1 := newElectorTester(0, "1", elector)
	value2 := newElectorTester(0, "2", elector)

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	err = elector.ChangeLeaderTo(0, "2", "3")
	assert.NotNil(t, err, "only leader node can transfer leader")

	err = elector.ChangeLeaderTo(0, "1", "2")
	assert.Nil(t, err, "change leader failed")

	time.Sleep(time.Second + time.Millisecond*200)
	assert.False(t, value1.isLeader(), "value1 must be follower")
	assert.True(t, value2.isLeader(), "value2 must be leader")
}

func TestCurrentLeader(t *testing.T) {
	stopC, port, err := startTestSingleEtcd(t)
	if err != nil {
		assert.FailNowf(t, "start embed etcd failed", "error: %+v", err)
	}
	defer close(stopC)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(fmt.Sprintf("http://127.0.0.1:%d", port), ","),
		DialTimeout: DefaultTimeout,
	})
	assert.Nil(t, err, "create etcd client failed")
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")
	defer elector.Stop(0)

	value1 := newElectorTester(0, "1", elector)
	value2 := newElectorTester(0, "2", elector)

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	data, err := elector.CurrentLeader(0)
	assert.Nil(t, err, "get current leader failed")
	assert.Equal(t, "1", string(data), "current leader failed")

	elector.ChangeLeaderTo(0, "1", "2")
	assert.Nil(t, err, "get current leader failed")

	time.Sleep(time.Second + time.Millisecond*200)
	data, err = elector.CurrentLeader(0)
	assert.Nil(t, err, "get current leader failed")
	assert.Equalf(t, "2", string(data), "current leader failed, %+v", value2.isLeader())
}

func TestStop(t *testing.T) {
	stopC, port, err := startTestSingleEtcd(t)
	if err != nil {
		assert.FailNowf(t, "start embed etcd failed", "error: %+v", err)
	}
	defer close(stopC)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(fmt.Sprintf("http://127.0.0.1:%d", port), ","),
		DialTimeout: DefaultTimeout,
	})
	assert.Nil(t, err, "create etcd client failed")
	defer client.Close()

	e, err := NewElector(client,
		WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")
	defer e.Stop(0)

	value1 := newElectorTester(0, "1", e)
	value2 := newElectorTester(0, "2", e)

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	e.Stop(0)

	assert.Equal(t, 0, len(e.(*elector).watchers), "watchers must be clear")
	assert.Equal(t, 0, len(e.(*elector).leasors), "leasors must be clear")
}

func TestDoIfLeader(t *testing.T) {
	stopC, port, err := startTestSingleEtcd(t)
	if err != nil {
		assert.FailNowf(t, "start embed etcd failed", "error: %+v", err)
	}
	defer close(stopC)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(fmt.Sprintf("http://127.0.0.1:%d", port), ","),
		DialTimeout: DefaultTimeout,
	})
	assert.Nil(t, err, "create etcd client failed")
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")

	value1 := newElectorTester(0, "1", elector)
	value2 := newElectorTester(0, "2", elector)

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	ok, err := elector.DoIfLeader(0, "1", nil, clientv3.OpPut("/key1", "value1"))
	assert.Nil(t, err, "check do if leader failed")
	assert.True(t, ok, "check do if leader failed")

	ok, err = elector.DoIfLeader(0, "2", nil, clientv3.OpPut("/key2", "value2"))
	assert.Nil(t, err, "check do if leader failed")
	assert.False(t, ok, "check do if leader failed")

	ok, err = elector.DoIfLeader(0, "1", []clientv3.Cmp{clientv3.Value("value2")}, clientv3.OpPut("/key1", "value2"))
	assert.Nil(t, err, "check do if leader failed")
	assert.False(t, ok, "check do if leader failed")

	ok, err = elector.DoIfLeader(0, "1", []clientv3.Cmp{clientv3.Compare(clientv3.Value("/key1"), "=", "value1")}, clientv3.OpPut("/key1", "value2"))
	assert.Nil(t, err, "check do if leader failed")
	assert.True(t, ok, "check do if leader failed")
}
