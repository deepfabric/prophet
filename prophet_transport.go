package prophet

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fagongzi/goetty"
)

var (
	bizCodec = &codec{}
)

const (
	typeResourceHeartbeatReq byte = iota
	typeResourceHeartbeatRsp
	typeContainerHeartbeatReq
	typeContainerHeartbeatRsp
	typeErrorRsp
)

type codecMsg struct{}

func (msg *codecMsg) Marshal() ([]byte, error) {
	return json.Marshal(msg)
}

func (msg *codecMsg) Unmarshal(data []byte) error {
	return json.Unmarshal(data, msg)
}

// ResourceHeartbeatReq resource hb msg
type ResourceHeartbeatReq struct {
	codecMsg
	Resource     Resource     `json:"resource"`
	LeaderPeer   *Peer        `json:"leaderPeer"`
	DownPeers    []*PeerStats `json:"downPeers"`
	PendingPeers []*Peer      `json:"pendingPeers"`
}

type resourceHeartbeatRsp struct {
	codecMsg
	ResourceID      uint64         `json:"resourceID"`
	NewLeaderPeerID uint64         `json:"newLeaderPeerID"`
	TargetPeerID    uint64         `json:"targetPeerID"`
	ChangeType      ChangePeerType `json:"changeType"`
}

func newChangeLeaderRsp(resourceID uint64, newLeaderPeerID uint64) *resourceHeartbeatRsp {
	return &resourceHeartbeatRsp{
		ResourceID:      resourceID,
		NewLeaderPeerID: newLeaderPeerID,
	}
}

func newChangePeerRsp(resourceID uint64, targetPeerID uint64, changeType ChangePeerType) *resourceHeartbeatRsp {
	return &resourceHeartbeatRsp{
		ResourceID:   resourceID,
		TargetPeerID: targetPeerID,
		ChangeType:   changeType,
	}
}

// ContainerHeartbeatReq container hb msg
type ContainerHeartbeatReq struct {
	codecMsg
	Container          Container `json:"container"`
	StorageCapacity    uint64    `json:"storageCapacity"`
	StorageAvailable   uint64    `json:"storageAvailable"`
	LeaderCount        uint64    `json:"leaderCount"`
	ReplicaCount       uint64    `json:"replicaCount"`
	SendingSnapCount   uint64    `json:"sendingSnapCount"`
	ReceivingSnapCount uint64    `json:"receivingSnapCount"`
	ApplyingSnapCount  uint64    `json:"applyingSnapCount"`
	Busy               bool      `json:"busy"`
}

type containerHeartbeatRsp struct {
	codecMsg
	status string `json:"status"`
}

func newContainerHeartbeatRsp() Serializable {
	rsp := &containerHeartbeatRsp{}
	rsp.status = "OK"

	return rsp
}

type errorRsp struct {
	codecMsg
	err error `json:"err"`
}

func newErrorRsp(err error) Serializable {
	rsp := &errorRsp{}
	rsp.err = err

	return rsp
}

// codec format: length(4bytes) + msgType(1bytes) + msg(length bytes)
type codec struct{}

func (c *codec) Encode(data interface{}, out *goetty.ByteBuf) error {
	var target Serializable
	var t byte

	if msg, ok := data.(*ResourceHeartbeatReq); ok {
		target = msg
		t = typeResourceHeartbeatReq
	} else if msg, ok := data.(*resourceHeartbeatRsp); ok {
		target = msg
		t = typeResourceHeartbeatRsp
	} else if msg, ok := data.(*ContainerHeartbeatReq); ok {
		target = msg
		t = typeContainerHeartbeatReq
	} else if msg, ok := data.(*containerHeartbeatRsp); ok {
		target = msg
		t = typeContainerHeartbeatRsp
	} else if msg, ok := data.(*errorRsp); ok {
		target = msg
		t = typeErrorRsp
	} else {
		return fmt.Errorf("not support msg: %+v", data)
	}

	value, err := target.Marshal()
	if err != nil {
		return err
	}

	out.WriteByte(t)
	out.Write(value)
	return nil
}

func (c *codec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	value := in.GetMarkedRemindData()
	in.MarkedBytesReaded()

	var msg Serializable
	t := value[0]

	switch t {
	case typeResourceHeartbeatReq:
		msg = &ResourceHeartbeatReq{}
		break
	case typeResourceHeartbeatRsp:
		msg = &resourceHeartbeatRsp{}
		break
	case typeContainerHeartbeatReq:
		msg = &ContainerHeartbeatReq{}
		break
	case typeContainerHeartbeatRsp:
		msg = &containerHeartbeatRsp{}
		break
	case typeErrorRsp:
		msg = &errorRsp{}
		break
	default:
		return false, nil, fmt.Errorf("unknown msg type")
	}

	err := msg.Unmarshal(value[1:])
	if err != nil {
		return false, nil, err
	}

	return true, msg, nil
}

func (p *Prophet) startListen() {
	go func() {
		err := p.tcpL.Start(p.doConnection)
		if err != nil {
			log.Fatalf("prophet: listen at %s failure, errors:\n%+v",
				p.node.Addr,
				err)
		}
	}()

	<-p.tcpL.Started()
}

func (p *Prophet) doConnection(conn goetty.IOSession) error {
	for {
		value, err := conn.Read()
		if err != nil {
			return err
		}

		if !p.isLeader() {
			conn.WriteAndFlush(newErrorRsp(fmt.Errorf("not leader")))
			continue
		}

		if msg, ok := value.(*ResourceHeartbeatReq); ok {
			rsp, err := p.handleResourceHeartbeat(msg)
			if err != nil {
				conn.WriteAndFlush(newErrorRsp(err))
				continue
			}
			conn.WriteAndFlush(rsp)
		} else if msg, ok := value.(*ContainerHeartbeatReq); ok {
			p.handleContainerHeartbeat(msg)
			conn.WriteAndFlush(newContainerHeartbeatRsp())
		}
	}
}

func (p *Prophet) getLeaderClient() goetty.IOSession {
	var conn goetty.IOSession
	var err error
	for {
		l := p.leader
		addr := p.node.Addr
		if l != nil {
			addr = l.Addr
		}

		conn, err = p.createLeaderClient(addr)
		if err == nil {
			log.Errorf("prophet: create leader connection failed, errors: %+v", err)
			break
		}

		time.Sleep(time.Second)
	}

	return conn
}

func (p *Prophet) createLeaderClient(leader string) (goetty.IOSession, error) {
	conn := goetty.NewConnector(leader,
		goetty.WithClientDecoder(goetty.NewIntLengthFieldBasedDecoder(bizCodec)),
		goetty.WithClientEncoder(goetty.NewIntLengthFieldBasedEncoder(bizCodec)),
		goetty.WithClientConnectTimeout(time.Second*10))
	_, err := conn.Connect()
	if err != nil {
		return nil, err
	}

	return conn, nil
}
