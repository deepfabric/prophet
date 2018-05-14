package prophet

import (
	"sync"

	"github.com/fagongzi/goetty"
)

// Prophet is the distributed scheduler and coordinator
type Prophet struct {
	sync.Mutex

	opts *options

	store       Store
	rt          *Runtime
	coordinator *Coordinator

	node       *Node
	leader     *Node
	leaderFlag int64
	signature  string

	tcpL      *goetty.Server
	runner    *Runner
	completeC chan struct{}
}

// NewProphet returns a prophet instance
func NewProphet(name string, addr string, opts ...Option) *Prophet {
	value := &options{cfg: &Cfg{}}
	for _, opt := range opts {
		opt(value)
	}
	value.adjust()

	p := new(Prophet)
	p.opts = value
	p.leaderFlag = 0
	p.node = &Node{
		ID:   p.opts.id,
		Name: name,
		Addr: addr,
	}
	p.signature = p.node.marshal()
	p.store = newEtcdStore(p.opts.client, p.opts.namespace)
	p.runner = NewRunner()
	p.coordinator = newCoordinator(p.runner, p.rt)
	p.tcpL = goetty.NewServer(addr,
		goetty.WithServerDecoder(goetty.NewIntLengthFieldBasedDecoder(bizCodec)),
		goetty.WithServerEncoder(goetty.NewIntLengthFieldBasedEncoder(bizCodec)))
	p.completeC = make(chan struct{})

	return p
}

// Start start the prophet
func (p *Prophet) Start() {
	p.startListen()
	p.startLeaderLoop()
	p.startResourceHeartbeatLoop()
	p.startContainerHeartbeatLoop()
}
