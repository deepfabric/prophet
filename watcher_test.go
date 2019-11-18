package prophet

import (
	"testing"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/stretchr/testify/assert"
)

func TestNotify(t *testing.T) {
	addr := "127.0.0.1:12345"
	bizCodec := &codec{}
	c := make(chan *EventNotify, 10)
	s := goetty.NewServer(addr,
		goetty.WithServerDecoder(goetty.NewIntLengthFieldBasedDecoder(bizCodec)),
		goetty.WithServerEncoder(goetty.NewIntLengthFieldBasedEncoder(bizCodec)))

	conns := 0
	go s.Start(func(conn goetty.IOSession) error {
		conns++
		for evt := range c {
			log.Infof("write %d", evt.Seq)
			conn.WriteAndFlush(evt)
		}

		log.Infof("exit")
		return nil
	})
	<-s.Started()

	defer s.Stop()

	w := NewWatcher("127.0.0.1:12345")
	ch := w.Watch(EventResourceChaned)
	go func() {
		for range ch {

		}
	}()

	c <- &EventNotify{Event: EventInit, Seq: 0}
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 1, conns, "test watcher failed")

	c <- &EventNotify{Event: EventResourceChaned, Seq: 2}
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, 2, conns, "test watcher failed")
}
