module github.com/deepfabric/prophet

go 1.15

require (
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf
	github.com/fagongzi/goetty v1.6.1
	github.com/fagongzi/util v0.0.0-20201116094402-221cc40c4593
	github.com/golang/mock v1.4.4
	github.com/stretchr/testify v1.3.0
	go.etcd.io/etcd v0.0.0-20201125193152-8a03d2e9614b
)

replace go.etcd.io/etcd => github.com/deepfabric/etcd v0.0.0-20201207015257-3b4a2ca4cf64
