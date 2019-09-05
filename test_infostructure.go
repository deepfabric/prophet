package prophet

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/coreos/etcd/embed"
)

func startTestSingleEtcd(port, peerPort int) (chan interface{}, error) {
	now := time.Now().Unix()

	cfg := embed.NewConfig()
	cfg.Name = "p1"
	cfg.Dir = fmt.Sprintf("%s/prophet/test-%d", os.TempDir(), now)
	cfg.WalDir = ""
	cfg.InitialCluster = fmt.Sprintf("p1=http://127.0.0.1:%d", peerPort)
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.EnablePprof = false
	cfg.Debug = false
	cfg.LPUrls, _ = parseUrls(fmt.Sprintf("http://127.0.0.1:%d", peerPort))
	cfg.APUrls = cfg.LPUrls
	cfg.LCUrls, _ = parseUrls(fmt.Sprintf("http://127.0.0.1:%d", port))
	cfg.ACUrls = cfg.LCUrls

	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-etcd.Server.ReadyNotify():
		stopC := make(chan interface{})
		go func() {
			<-stopC
			etcd.Server.Stop()
			os.RemoveAll(cfg.Dir)
		}()

		return stopC, nil
	case <-time.After(time.Minute * 5):
		return nil, errors.New("start embed etcd timeout")
	}
}
