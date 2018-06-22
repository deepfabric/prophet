package prophet

import (
	"sync"

	"github.com/fagongzi/goetty"
)

var (
	// EventInit event init
	EventInit = 1 << 1
	// EventResourceCreated event resource created
	EventResourceCreated = 1 << 2
	// EventResourceLeaderChanged event resource leader changed
	EventResourceLeaderChanged = 1 << 3
	// EventResourceChaned event resource changed
	EventResourceChaned = 1 << 4
	// EventResourcePeersChaned event resource peers changed
	EventResourcePeersChaned = 1 << 5

	// EventFlagResource all resource event
	EventFlagResource = EventResourceCreated | EventResourceLeaderChanged | EventResourceChaned | EventResourcePeersChaned
	// EventFlagAll all event
	EventFlagAll = 0xffffffff
)

// MatchEvent returns the flag has the target event
func MatchEvent(event, flag int) bool {
	return event == 0 || event&flag != 0
}

func (p *Prophet) notifyEvent(event *EventNotify) {
	if event == nil {
		return
	}

	if p.isLeader() {
		p.wn.onEvent(event)
	}
}

// InitWatcher init watcher
type InitWatcher struct {
	Flag int `json:"flag"`
}

// EventNotify event notify
type EventNotify struct {
	Event int    `json:"event"`
	Value []byte `json:"value"`
}

func newInitEvent(rt *Runtime) (*EventNotify, error) {
	value := goetty.NewByteBuf(512)

	resources := rt.GetResources()
	value.WriteInt(len(resources))

	for _, v := range resources {
		data, err := v.meta.Marshal()
		if err != nil {
			return nil, err
		}
		value.WriteInt(len(data))
		value.Write(data)
	}

	nt := &EventNotify{
		Event: EventInit,
		Value: value.RawBuf()[0:value.Readable()],
	}
	value.Release()
	return nt, nil
}

func newResourceEvent(event int, target Resource) *EventNotify {
	value, err := target.Marshal()
	if err != nil {
		return nil
	}

	return &EventNotify{
		Event: event,
		Value: value,
	}
}

func newLeaderChangerEvent(target, leader uint64) *EventNotify {
	value := goetty.NewByteBuf(16)
	value.WriteUint64(target)
	value.WriteUint64(leader)

	nt := &EventNotify{
		Event: EventResourceLeaderChanged,
		Value: value.RawBuf()[0:value.Readable()],
	}
	value.Release()
	return nt
}

// ReadInitEventValues read all resource info
func (evt *EventNotify) ReadInitEventValues() [][]byte {
	if len(evt.Value) == 0 {
		return nil
	}

	buf := goetty.NewByteBuf(len(evt.Value))
	buf.Write(evt.Value)
	n, _ := buf.ReadInt()
	value := make([][]byte, n, n)
	for i := 0; i < n; i++ {
		size, _ := buf.ReadInt()
		_, value[i], _ = buf.ReadBytes(size)
	}
	buf.Release()
	return value
}

// ReadLeaderChangerValue returns the target resource and the new leader
// returns resourceid, newleaderid
func (evt *EventNotify) ReadLeaderChangerValue() (uint64, uint64) {
	if len(evt.Value) == 0 {
		return 0, 0
	}
	buf := goetty.NewByteBuf(len(evt.Value))
	buf.Write(evt.Value)
	resourceID, _ := buf.ReadUInt64()
	newLeaderID, _ := buf.ReadUInt64()
	buf.Release()

	return resourceID, newLeaderID
}

type watcher struct {
	info *InitWatcher
	conn goetty.IOSession
}

func (wt *watcher) notify(evt *EventNotify) {
	if MatchEvent(evt.Event, wt.info.Flag) {
		err := wt.conn.WriteAndFlush(evt)
		if err != nil {
			wt.conn.Close()
		}
	}
}

type watcherNotifier struct {
	sync.RWMutex

	watchers *sync.Map
	eventC   chan *EventNotify
	rt       *Runtime
}

func newWatcherNotifier(rt *Runtime) *watcherNotifier {
	return &watcherNotifier{
		watchers: &sync.Map{},
		eventC:   make(chan *EventNotify, 256),
		rt:       rt,
	}
}

func (wn *watcherNotifier) onEvent(evt *EventNotify) {
	wn.RLock()
	if wn.eventC != nil {
		wn.eventC <- evt
	}
	wn.RUnlock()
}

func (wn *watcherNotifier) onInitWatcher(msg *InitWatcher, conn goetty.IOSession) {
	wn.Lock()
	defer wn.Unlock()

	log.Infof("prophet: new watcher %s added", conn.RemoteIP())

	if wn.eventC != nil {
		nt, err := newInitEvent(wn.rt)
		if err != nil {
			log.Errorf("prophet: marshal init notify failed, errors:%+v", err)
			conn.Close()
			return
		}

		err = conn.WriteAndFlush(nt)
		if err != nil {
			log.Errorf("prophet: notify to %s failed, errors:%+v",
				conn.RemoteIP(),
				err)
			conn.Close()
			return
		}

		wn.watchers.Store(conn.ID(), &watcher{
			info: msg,
			conn: conn,
		})
	}
}

func (wn *watcherNotifier) clearWatcher(conn goetty.IOSession) {
	log.Infof("prophet: clear watcher %s", conn.RemoteIP())
	wn.watchers.Delete(conn.ID())
}

func (wn *watcherNotifier) stop() {
	wn.Lock()
	close(wn.eventC)
	wn.eventC = nil
	wn.watchers.Range(func(key, value interface{}) bool {
		wn.watchers.Delete(key)
		return true
	})
	wn.Unlock()
	log.Infof("prophet: watcher notifyer stopped")
}

func (wn *watcherNotifier) start() {
	for {
		evt, ok := <-wn.eventC
		if !ok {
			log.Infof("prophet: watcher notifer exited")
			return
		}

		log.Debugf("prophet: new event: %+v", evt)
		wn.watchers.Range(func(key, value interface{}) bool {
			wt := value.(*watcher)
			wt.notify(evt)
			return true
		})
	}
}
