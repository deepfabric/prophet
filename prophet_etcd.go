package prophet

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/pkg/capnslog"
)

// unixToHTTP replace unix scheme with http.
var unixToHTTP = strings.NewReplacer("unix://", "http://", "unixs://", "http://")

var (
	maxCheckEtcdRunningCount = 60 * 10
	checkEtcdRunningDelay    = 1 * time.Second
)

// EmbeddedEtcdCfg cfg for embedded etcd
type EmbeddedEtcdCfg struct {
	Name         string
	DataPath     string
	Join         string
	EmbedEtcdLog string

	URLsClient          string
	URLsAdvertiseClient string

	URLsPeer          string
	URLsAdvertisePeer string

	InitialCluster      string
	InitialClusterState string
}

func (c *EmbeddedEtcdCfg) getEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataPath
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.EnablePprof = false
	cfg.Debug = false

	var err error
	cfg.LPUrls, err = parseUrls(c.URLsPeer)
	if err != nil {
		return nil, err
	}

	cfg.APUrls, err = parseUrls(getStringValue(c.URLsAdvertisePeer, c.URLsPeer))
	if err != nil {
		return nil, err
	}

	cfg.LCUrls, err = parseUrls(c.URLsClient)
	if err != nil {
		return nil, err
	}

	cfg.ACUrls, err = parseUrls(getStringValue(c.URLsAdvertiseClient, c.URLsClient))
	if err != nil {
		return nil, err
	}

	if cfg.ClusterState == "" {
		cfg.ClusterState = embed.ClusterStateFlagNew
	}

	if cfg.InitialCluster == "" {
		addrs := []string{}

		for _, u := range cfg.APUrls {
			addrs = append(addrs, fmt.Sprintf("%s=%s", c.Name, u.String()))
		}

		cfg.InitialCluster = strings.Join(addrs, ",")
	}

	return cfg, nil
}

func initWithEmbedEtcd(ecfg *EmbeddedEtcdCfg, opts *options) {
	if ecfg.EmbedEtcdLog != "" {
		f, err := os.OpenFile(ecfg.EmbedEtcdLog, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log.Fatal("redirect embed log to %s failed with %+v", ecfg.EmbedEtcdLog, err)
		}
		capnslog.SetFormatter(capnslog.NewPrettyFormatter(f, false))
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	}

	err := prepareJoin(ecfg)
	if err != nil {
		log.Fatalf("prophet: prepare join embed etcd failed with %+v",
			err)
	}

	log.Info("prophet: start embed etcd")
	cfg, err := ecfg.getEmbedEtcdConfig()
	if err != nil {
		log.Fatalf("prophet: start embed etcd failed with %+v",
			err)
	}

	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatalf("prophet: start embed etcd failed with %+v",
			err)
	}

	select {
	case <-etcd.Server.ReadyNotify():
		log.Info("prophet: embed etcd is ready")
		doAfterEmbedEtcdServerReady(etcd, cfg, ecfg, opts)
	case <-time.After(time.Minute * 5):
		log.Fatalf("prophet: start embed etcd timeout")
	}
}

func doAfterEmbedEtcdServerReady(etcd *embed.Etcd, cfg *embed.Config, ecfg *EmbeddedEtcdCfg, opts *options) {
	checkEtcdCluster(etcd, ecfg)

	id := uint64(etcd.Server.ID())
	log.Infof("prophet: embed server ids, current %X, leader %X",
		id,
		etcd.Server.Leader())

	client, err := initEtcdClient(ecfg)
	if err != nil {
		log.Fatalf("prophet: init embed etcd client failure, errors:\n %+v",
			err)
	}

	updateAdvertisePeerUrls(id, client, ecfg)
	if err := waitEtcdStart(cfg, client); err != nil {
		// See https://github.com/coreos/etcd/issues/6067
		// Here may return "not capable" error because we don't start
		// all etcds in initial_cluster at same time, so here just log
		// an error.
		// Note that pd can not work correctly if we don't start all etcds.
		log.Fatalf("prophet: etcd start failure, errors:\n%+v", err)
	}

	opts.client = client
	// update client endpoint
	go func() {
		for {
			members, err := getCurrentClusterMembers(client)
			if err != nil {
				log.Fatalf("prophet: get current members of etcd cluster failed with %+v", err)
			}

			var eps []string
			for _, m := range members.Members {
				eps = append(eps, m.GetClientURLs()...)
			}
			client.SetEndpoints(eps...)
			log.Debugf("prophet: etcd client endpoints set to %+v", eps)

			time.Sleep(time.Second * 10)
		}
	}()
}

func initEtcdClient(ecfg *EmbeddedEtcdCfg) (*clientv3.Client, error) {
	clientAddrs := strings.Split(getStringValue(ecfg.URLsAdvertiseClient, ecfg.URLsClient), ",")
	if ecfg.Join != "" {
		clientAddrs = strings.Split(ecfg.Join, ",")
	}

	log.Infof("prophet: create etcd v3 client with endpoints <%v>", clientAddrs)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   clientAddrs,
		DialTimeout: DefaultTimeout,
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

func updateAdvertisePeerUrls(id uint64, client *clientv3.Client, cfg *EmbeddedEtcdCfg) {
	members, err := getCurrentClusterMembers(client)
	if err != nil {
		log.Fatalf("prophet: update current members of etcd cluster")
	}

	for _, m := range members.Members {
		if id == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if cfg.URLsAdvertisePeer != etcdPeerURLs {
				log.Infof("prophet: update advertise peer urls from <%s> to <%s>",
					cfg.URLsAdvertisePeer,
					etcdPeerURLs)
				cfg.URLsAdvertisePeer = etcdPeerURLs
			}
		}
	}
}

func waitEtcdStart(cfg *embed.Config, client *clientv3.Client) error {
	var err error
	for i := 0; i < maxCheckEtcdRunningCount; i++ {
		// etcd may not start ok, we should wait and check again
		_, err = endpointStatus(cfg, client)
		if err == nil {
			return nil
		}

		time.Sleep(checkEtcdRunningDelay)
		continue
	}

	return err
}

func endpointStatus(cfg *embed.Config, c *clientv3.Client) (*clientv3.StatusResponse, error) {
	endpoint := []string{cfg.LCUrls[0].String()}[0]

	m := clientv3.NewMaintenance(c)
	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Ctx(), DefaultRequestTimeout)
	resp, err := m.Status(ctx, endpoint)
	cancel()

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("prophet: check etcd status failed, endpoint=<%s> resp=<%+v> cost<%s> errors:\n %+v",
			endpoint,
			resp,
			cost,
			err)
	}

	return resp, err
}

func checkEtcdCluster(etcd *embed.Etcd, cfg *EmbeddedEtcdCfg) {
	um, err := types.NewURLsMap(cfg.InitialCluster)
	if err != nil {
		log.Fatalf("prophet: check embed etcd failure, errors:\n %+v",
			err)
	}

	err = checkClusterID(etcd.Server.Cluster().ID(), um)
	if err != nil {
		log.Fatalf("prophet: check embed etcd failure, errors:\n %+v",
			err)
	}
}

func checkClusterID(localClusterID types.ID, um types.URLsMap) error {
	if len(um) == 0 {
		return nil
	}

	var peerURLs []string
	for _, urls := range um {
		peerURLs = append(peerURLs, urls.StringSlice()...)
	}

	for i, u := range peerURLs {
		u, gerr := url.Parse(u)
		if gerr != nil {
			return gerr
		}

		trp := newHTTPTransport(u.Scheme)

		// For tests, change scheme to http.
		// etcdserver/api/v3rpc does not recognize unix protocol.
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			peerURLs[i] = unixToHTTP.Replace(peerURLs[i])
		}

		remoteCluster, gerr := etcdserver.GetClusterFromRemotePeers([]string{peerURLs[i]}, trp)
		trp.CloseIdleConnections()
		if gerr != nil {
			// Do not return error, because other members may be not ready.
			log.Warnf("bootstrap: check etcd embed, may be member is not ready, member=<%s>",
				u)
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return fmt.Errorf("embed etcd cluster id not match, expect <%d> got=<%d>",
				localClusterID,
				remoteClusterID)
		}
	}

	return nil
}

func newHTTPTransport(scheme string) *http.Transport {
	tr := &http.Transport{}
	if scheme == "unix" || scheme == "unixs" {
		tr.Dial = unixDial
	}
	return tr
}

func unixDial(_, addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}
