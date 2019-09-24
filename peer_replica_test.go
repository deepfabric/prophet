package prophet

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestCreatePeerReplica(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

	meta := newTestContainer()
	meta.CID = 1

	res := newTestResource()
	res.ResID = 2
	res.ResPeers = append(res.ResPeers, &Peer{
		ID:          10000,
		ContainerID: meta.CID,
	})

	store := newTestResourceStore(ctrl, meta)
	pr, err := CreatePeerReplica(store, res, nil, elector)
	assert.Nil(t, err, "create peer replica failed")
	assert.Equal(t, uint64(10000), pr.peer.ID, "create peer replica failed")
}

func TestPeerReplicaElector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

	meta1 := newTestContainer()
	meta1.CID = 1
	res1 := newTestResource()
	res1.ResID = 1001
	res1.ResPeers = append(res1.ResPeers, &Peer{
		ID:          10001,
		ContainerID: meta1.CID,
	})
	store1 := newTestResourceStore(ctrl, meta1)
	pr1, err := CreatePeerReplica(store1, res1, newTestPeerReplicaHandler(ctrl), elector)
	assert.Nil(t, err, "create peer replica failed")
	go func() {
		for {
			if !pr1.handleEvent() {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()

	time.Sleep(time.Second + time.Millisecond*100)

	leader := false
	pr1.Do(func(err error) {
		leader = pr1.IsLeader()
	}, time.Second)
	assert.True(t, leader, "pr1 must be leader")

	meta2 := newTestContainer()
	meta2.CID = 2
	res2 := newTestResource()
	res2.ResID = 1002
	res2.ResPeers = append(res2.ResPeers, &Peer{
		ID:          10002,
		ContainerID: meta2.CID,
	})
	store2 := newTestResourceStore(ctrl, meta2)
	pr2, err := CreatePeerReplica(store2, res2, newTestPeerReplicaHandler(ctrl), elector)
	assert.Nil(t, err, "create peer replica failed")

	time.Sleep(time.Second + time.Millisecond*100)
	pr2.Do(func(err error) {
		leader = pr2.IsLeader()
	}, time.Second)
	assert.False(t, leader, "pr2 must be follower")
}
