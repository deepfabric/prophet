package prophet

import (
	"time"

	"github.com/coreos/etcd/clientv3"
)

type options struct {
	namespace string
	leaseTTL  int64
	id        uint64
	client    *clientv3.Client

	resourceHBFetch   func() []*ResourceHeartbeatReq
	resourceInterval  time.Duration
	containerHBFetch  func() *ContainerHeartbeatReq
	containerInterval time.Duration

	hbHandler HeartbeatHandler

	cfg *Cfg
}

func (opts *options) adjust() {
	if opts.leaseTTL == 0 {
		opts.leaseTTL = 5
	}

	if opts.namespace == "" {
		opts.namespace = "/prophet"
	}

	if opts.client == nil {
		log.Fatalf("etcd v3 client is not setting, using WithExternalEtcd or WithEmbeddedEtcd to initialize.")
	}

	if opts.hbHandler == nil {
		log.Fatalf("heartbeat handler must be set.")
	}

	opts.cfg.adujst()
	cfg = opts.cfg
}

// Option is prophet create option
type Option func(*options)

// WithExternalEtcd using  external etcd cluster
func WithExternalEtcd(id uint64, client *clientv3.Client) Option {
	return func(opts *options) {
		opts.client = client
		opts.id = id
	}
}

// WithEmbeddedEtcd using embedded etcd cluster
func WithEmbeddedEtcd(cfg *EmbeddedEtcdCfg) Option {
	return func(opts *options) {
		opts.id, opts.client = initWithEmbedEtcd(cfg)
	}
}

// WithLeaseTTL prophet leader lease ttl
func WithLeaseTTL(leaseTTL int64) Option {
	return func(opts *options) {
		opts.leaseTTL = leaseTTL
	}
}

// WithResourceHeartbeat fetch resource info
func WithResourceHeartbeat(fetch func() []*ResourceHeartbeatReq, interval time.Duration) Option {
	return func(opts *options) {
		opts.resourceHBFetch = fetch
		opts.resourceInterval = interval
	}
}

// WithContainerHeartbeat fetch container info
func WithContainerHeartbeat(fetch func() *ContainerHeartbeatReq, interval time.Duration) Option {
	return func(opts *options) {
		opts.containerHBFetch = fetch
		opts.containerInterval = interval
	}
}

// WithMaxScheduleRetries using MaxScheduleRetries maximum retry times for schedule
func WithMaxScheduleRetries(value int) Option {
	return func(opts *options) {
		opts.cfg.MaxScheduleRetries = value
	}
}

// WithMaxScheduleInterval using MaxScheduleInterval maximum schedule interval per scheduler
func WithMaxScheduleInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxScheduleInterval = value
	}
}

// WithMinScheduleInterval using MinScheduleInterval minimum schedule interval per scheduler
func WithMinScheduleInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MinScheduleInterval = value
	}
}

// WithTimeoutWaitOperatorComplete timeout for waitting teh operator complete
func WithTimeoutWaitOperatorComplete(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.TimeoutWaitOperatorComplete = value
	}
}

// WithMaxFreezeScheduleInterval freeze the container for a while if shouldSchedule is returns false
func WithMaxFreezeScheduleInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxFreezeScheduleInterval = value
	}
}

// WithMaxAllowContainerDownDuration maximum down time of removed from replicas
func WithMaxAllowContainerDownDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxAllowContainerDownDuration = value
	}
}

// WithMaxRebalanceLeader maximum count of transfer leader operator
func WithMaxRebalanceLeader(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRebalanceLeader = value
	}
}

// WithMaxRebalanceReplica maximum count of remove|add replica operator
func WithMaxRebalanceReplica(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRebalanceReplica = value
	}
}

// WithMaxScheduleReplica maximum count of schedule replica operator
func WithMaxScheduleReplica(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxScheduleReplica = value
	}
}

// WithCountResourceReplicas replica number per resource
func WithCountResourceReplicas(value int) Option {
	return func(opts *options) {
		opts.cfg.CountResourceReplicas = value
	}
}

// WithMinAvailableStorageUsedRate minimum storage used rate of container, if the rate is over this value, skip the container
func WithMinAvailableStorageUsedRate(value int) Option {
	return func(opts *options) {
		opts.cfg.MinAvailableStorageUsedRate = value
	}
}

// WithMaxLimitSnapshotsCount maximum count of node about snapshot
func WithMaxLimitSnapshotsCount(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxLimitSnapshotsCount = value
	}
}

// WithLocationLabels the label used for location
func WithLocationLabels(value []string) Option {
	return func(opts *options) {
		opts.cfg.LocationLabels = value
	}
}

// WithResourceFactory resource factory method
func WithResourceFactory(value func() Resource) Option {
	return func(opts *options) {
		opts.cfg.resourceFactory = value
	}
}

// WithContainerFactory container factory method
func WithContainerFactory(value func() Container) Option {
	return func(opts *options) {
		opts.cfg.containerFactory = value
	}
}

// WithScheduler add a scheduler
func WithScheduler(value Scheduler) Option {
	return func(opts *options) {
		opts.cfg.schedulers = append(opts.cfg.schedulers, value)
	}
}

// WithHeartbeatHandler using a heartbeat handler
func WithHeartbeatHandler(handler HeartbeatHandler) Option {
	return func(opts *options) {
		opts.hbHandler = handler
	}
}
