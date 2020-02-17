package prophet

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
)

func TestAlreadyBootstrapped(t *testing.T) {
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

	e, err := NewElector(client)
	assert.Nil(t, err, "TestAlreadyBootstrapped failed")
	defer e.Stop(math.MaxUint64)

	go e.ElectionLoop(context.Background(), math.MaxUint64, "node1", func() {}, func() {})
	time.Sleep(time.Millisecond * 200)

	store := newEtcdStore(client, nil, "node1", e)
	yes, err := store.AlreadyBootstrapped()
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.False(t, yes, "TestAlreadyBootstrapped failed")

	yes, err = store.PutBootstrapped(newTestContainer(), newTestResource())
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.True(t, yes, "TestAlreadyBootstrapped failed")

	yes, err = store.AlreadyBootstrapped()
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.True(t, yes, "TestAlreadyBootstrapped failed")
}
