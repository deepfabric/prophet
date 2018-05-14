package prophet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

const (
	// DefaultTimeout default timeout
	DefaultTimeout = time.Second * 3
	// DefaultRequestTimeout default request timeout
	DefaultRequestTimeout = 10 * time.Second
	// DefaultSlowRequestTime default slow request time
	DefaultSlowRequestTime = time.Second * 1
)

var (
	errMaybeNotLeader = errors.New("may be not leader")
	errTxnFailed      = errors.New("failed to commit transaction")

	errSchedulerExisted  = errors.New("scheduler is existed")
	errSchedulerNotFound = errors.New("scheduler is not found")
)

var (
	endID = uint64(math.MaxUint64)
)

func getCurrentClusterMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	members, err := client.MemberList(ctx)
	cancel()

	return members, err
}

type etcdStore struct {
	client        *clientv3.Client
	idPath        string
	leaderPath    string
	resourcePath  string
	containerPath string
}

func newEtcdStore(client *clientv3.Client, namespace string) Store {
	return &etcdStore{
		client:        client,
		idPath:        fmt.Sprintf("%s/meta/id", namespace),
		leaderPath:    fmt.Sprintf("%s/meta/leader", namespace),
		resourcePath:  fmt.Sprintf("%s/meta/resources", namespace),
		containerPath: fmt.Sprintf("%s/meta/containers", namespace),
	}
}

func (s *etcdStore) GetCurrentLeader() (*Node, error) {
	resp, err := s.getValue(s.leaderPath)
	if err != nil {
		return nil, err
	}

	if nil == resp {
		return nil, nil
	}

	v := &Node{}
	err = json.Unmarshal(resp, v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (s *etcdStore) ResignLeader(leaderSignature string) error {
	resp, err := s.leaderTxn(leaderSignature).Then(clientv3.OpDelete(s.leaderPath)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (s *etcdStore) WatchLeader() {
	watcher := clientv3.NewWatcher(s.client)
	defer watcher.Close()

	ctx := s.client.Ctx()
	for {
		rch := watcher.Watch(ctx, s.leaderPath)
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

func (s *etcdStore) CampaignLeader(leaderSignature string, leaderLeaseTTL int64, enableLeaderFun, disableLeaderFun func()) error {
	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, leaderLeaseTTL)
	cancel()

	if cost := time.Now().Sub(start); cost > DefaultSlowRequestTime {
		log.Warnf("prophet: lessor grants too slow, cost=<%s>", cost)
	}

	if err != nil {
		return err
	}

	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(s.leaderPath), "=", 0)).
		Then(clientv3.OpPut(s.leaderPath, leaderSignature, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	// Make the leader keepalived.
	ch, err := lessor.KeepAlive(s.client.Ctx(), clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		return err
	}

	enableLeaderFun()
	defer disableLeaderFun()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("prophet: channel that keep alive for leader lease is closed")
				return nil
			}
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
		}
	}
}

// PutContainer returns nil if container is add or update succ
func (s *etcdStore) PutContainer(meta Container) error {
	key := s.getKey(meta.ID(), s.containerPath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.save(key, string(data))
}

// PutResource returns nil if resource is add or update succ
func (s *etcdStore) PutResource(meta Resource) error {
	key := s.getKey(meta.ID(), s.resourcePath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.save(key, string(data))
}

func (s *etcdStore) LoadResources(limit int64, do func(Resource)) error {
	startID := uint64(0)
	endKey := s.getKey(endID, s.resourcePath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := s.getKey(startID, s.resourcePath)
		resp, err := s.get(startKey, withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			v := cfg.resourceFactory()
			err := v.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			startID = v.ID() + 1
			do(v)
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (s *etcdStore) LoadContainers(limit int64, do func(Container)) error {
	startID := uint64(0)
	endKey := s.getKey(endID, s.containerPath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := s.getKey(startID, s.containerPath)
		resp, err := s.get(startKey, withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			v := cfg.containerFactory()
			err := v.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			startID = v.ID() + 1
			do(v)
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (s *etcdStore) AllocID() (uint64, error) {
	return 0, nil
}

func (s *etcdStore) getKey(id uint64, base string) string {
	return fmt.Sprintf("%s/%020d", base, id)
}

func (s *etcdStore) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

func (s *etcdStore) leaderTxn(leaderSignature string, cs ...clientv3.Cmp) clientv3.Txn {
	return newSlowLogTxn(s.client).If(append(cs, s.leaderCmp(leaderSignature))...)
}

func (s *etcdStore) leaderCmp(leaderSignature string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(s.leaderPath), "=", leaderSignature)
}

func (s *etcdStore) getValue(key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := s.get(key, opts...)
	if err != nil {
		return nil, err
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, fmt.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, nil
}

func (s *etcdStore) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(s.client).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("prophet: read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, err
	}

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("prophet: read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}

func (s *etcdStore) save(key, value string) error {
	resp, err := s.txn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *etcdStore) create(key, value string) error {
	resp, err := s.txn().If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *etcdStore) delete(key string, opts ...clientv3.OpOption) error {
	resp, err := s.txn().Then(clientv3.OpDelete(key, opts...)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	return &slowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

func (t *slowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.If(cs...),
		cancel: t.cancel,
	}
}

func (t *slowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.Then(ops...),
		cancel: t.cancel,
	}
}

// Commit implements Txn Commit interface.
func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Now().Sub(start)
	if cost > DefaultSlowRequestTime {
		log.Warnf("prophet: txn runs too slow, resp=<%+v> cost=<%s> errors:\n %+v",
			resp,
			cost,
			err)
	}

	return resp, err
}
