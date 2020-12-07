package prophet

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

func TestSetAndGet(t *testing.T) {
	store := newMemLocalStorage()

	key1 := []byte("key1")
	value1 := []byte("value1")

	key2 := []byte("key2")
	value2 := []byte("value2")

	err := store.Set(key1, value1, key2, value2)
	assert.Nil(t, err, "local storage set failed")

	value, err := store.Get(key1)
	assert.Nil(t, err, "local storage get failed")
	assert.Equal(t, string(value1), string(value), "local storage get failed")

	value, err = store.Get(key2)
	assert.Nil(t, err, "local storage get failed")
	assert.Equal(t, string(value2), string(value), "local storage get failed")

	value, err = store.Get([]byte("key3"))
	assert.Nil(t, err, "local storage get failed")
	assert.Equal(t, 0, len(value), "local storage get failed")
}

func TestRemove(t *testing.T) {
	store := newMemLocalStorage()

	key1 := []byte("key1")
	value1 := []byte("value1")

	key2 := []byte("key2")
	value2 := []byte("value2")

	key3 := []byte("key3")
	value3 := []byte("value3")

	store.Set(key1, value1, key2, value2, key3, value3)

	err := store.Remove(key1, key2)
	assert.Nil(t, err, "local storage remove failed")

	value, _ := store.Get([]byte("key1"))
	assert.Equal(t, 0, len(value), "local storage remove failed")

	value, _ = store.Get([]byte("key2"))
	assert.Equal(t, 0, len(value), "local storage remove failed")

	value, _ = store.Get([]byte("key3"))
	assert.Equal(t, string(value3), string(value), "local storage remove failed")
}

func TestRange(t *testing.T) {
	store := newMemLocalStorage()

	key1 := []byte("key1")
	value1 := []byte("value1")

	key2 := []byte("key2")
	value2 := []byte("value2")

	key3 := []byte("key3")
	value3 := []byte("value3")

	key4 := []byte("key4")
	value4 := []byte("value4")

	store.Set(key4, value4, key3, value3, key2, value2, key1, value1)

	var values [][]byte
	fn := func(key, value []byte) bool {
		values = append(values, value)
		return true
	}

	err := store.Range([]byte("key"), 1, fn)
	assert.Nil(t, err, "local storage range failed")
	assert.Equal(t, 1, len(values), "local storage range failed")
	assert.Equal(t, string(value1), string(values[0]), "local storage range failed")

	values = values[:0]
	err = store.Range([]byte("key"), 4, fn)
	assert.Nil(t, err, "local storage range failed")
	assert.Equal(t, 4, len(values), "local storage range failed")
	assert.Equal(t, string(value1), string(values[0]), "local storage range failed")
	assert.Equal(t, string(value2), string(values[1]), "local storage range failed")
	assert.Equal(t, string(value3), string(values[2]), "local storage range failed")
	assert.Equal(t, string(value4), string(values[3]), "local storage range failed")

	values = values[:0]
	err = store.Range([]byte("key1"), 4, fn)
	assert.Nil(t, err, "local storage range failed")
	assert.Equal(t, 1, len(values), "local storage range failed")
	assert.Equal(t, string(value1), string(values[0]), "local storage range failed")

	values = values[:0]
	err = store.Range([]byte("abc"), 4, fn)
	assert.Nil(t, err, "local storage range failed")
	assert.Equal(t, 0, len(values), "local storage range failed")
}

func TestBootstrapCluster(t *testing.T) {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	elector, _ := NewElector(client)
	et := newElectorTester(math.MaxUint64, "c1", elector)
	et.start()
	defer et.stop(0)

	rpc := newTestRPC(ctrl, 0)
	pd := newTestProphet(ctrl, newEtcdStore(client, newTestAdapter(ctrl), "c1", elector), rpc, client, func() {})

	localDB := newMemLocalStorage()
	c := newTestContainer()
	store := NewLocalStore(c, localDB, pd)

	res := newTestResource()
	res2 := res.Clone()
	store.BootstrapCluster(res, res2)
	assert.Equalf(t, 1, len(res.Peers()), "bootstrap failed: %+v", res)
	assert.Equalf(t, 1, len(res2.Peers()), "bootstrap failed: %+v", res)

	var values []Resource
	store.MustLoadResources(func(data []byte) (uint64, error) {
		value := newTestResource()

		err := value.Unmarshal(data)
		if err != nil {
			return 0, err
		}

		values = append(values, value)
		return value.ResID, nil
	})

	assert.Equal(t, 2, len(values), "bootstrap failed")
	assert.Equal(t, 1, len(values[0].Peers()), "bootstrap failed")
	assert.Equal(t, 1, len(values[1].Peers()), "bootstrap failed")

	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must be panic!")
		}
	}()

	localDB.Remove(containerKey)
	store.BootstrapCluster(newTestResource())
}

func TestMustPutResource(t *testing.T) {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	elector, _ := NewElector(client)
	et := newElectorTester(math.MaxUint64, "c1", elector)
	et.start()
	defer et.stop(0)

	rpc := newTestRPC(ctrl, 0)
	pd := newTestProphet(ctrl, newEtcdStore(client, newTestAdapter(ctrl), "c1", elector), rpc, client, func() {})

	localDB := newMemLocalStorage()
	c := newTestContainer()
	store := NewLocalStore(c, localDB, pd)

	res := newTestResource()
	res2 := newTestResource()
	res.ResID = 1
	res2.ResID = 2

	store.MustPutResource(res, res2)
	assert.Equal(t, 2, store.MustCountResources(), "put resource failed")

	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must be panic!")
		}
	}()
	errRes := newTestResource()
	errRes.err = true
	store.MustPutResource(errRes)
}

func TestMustRemoveResource(t *testing.T) {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	elector, _ := NewElector(client)
	et := newElectorTester(math.MaxUint64, "c1", elector)
	et.start()
	defer et.stop(0)

	rpc := newTestRPC(ctrl, 0)
	pd := newTestProphet(ctrl, newEtcdStore(client, newTestAdapter(ctrl), "c1", elector), rpc, client, func() {})

	localDB := newMemLocalStorage()
	c := newTestContainer()
	store := NewLocalStore(c, localDB, pd)

	res := newTestResource()
	res2 := newTestResource()
	res.ResID = 1
	res2.ResID = 2

	store.MustPutResource(res, res2)
	assert.Equal(t, 2, store.MustCountResources(), "remove resource failed")

	store.MustRemoveResource(res.ResID)
	assert.Equal(t, 1, store.MustCountResources(), "remove resource failed")

	store.MustRemoveResource(res2.ResID)
	assert.Equal(t, 0, store.MustCountResources(), "remove resource failed")
}

func TestMustAllocID(t *testing.T) {
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

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	elector, _ := NewElector(client)
	et := newElectorTester(math.MaxUint64, "c1", elector)
	et.start()
	defer et.stop(0)

	rpc := newTestRPC(ctrl, 0)
	pd := newTestProphet(ctrl, newEtcdStore(client, newTestAdapter(ctrl), "c1", elector), rpc, client, func() {})

	localDB := newMemLocalStorage()
	c := newTestContainer()
	store := NewLocalStore(c, localDB, pd)

	assert.Equal(t, uint64(1), store.MustAllocID(), "alloc id failed")
	assert.Equal(t, uint64(2), store.MustAllocID(), "alloc id failed")
	assert.Equal(t, uint64(3), store.MustAllocID(), "alloc id failed")
}
