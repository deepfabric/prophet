package prophet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandResource(t *testing.T) {
	m := make(map[uint64]*ResourceRuntime)

	res := newTestResource()
	res.ResID = 2
	m[2] = &ResourceRuntime{
		meta:       res,
		leaderPeer: &Peer{},
	}

	res = newTestResource()
	res.ResID = 10
	m[10] = &ResourceRuntime{
		meta:       res,
		leaderPeer: &Peer{},
	}

	res = newTestResource()
	res.ResID = 1
	m[1] = &ResourceRuntime{
		meta:       res,
		leaderPeer: &Peer{},
	}

	value := randResource(m, ReplicaKind, func(res1, res2 Resource) int {
		if res1.ID() == res2.ID() {
			return 0
		} else if res1.ID() > res2.ID() {
			return 1
		} else {
			return -1
		}
	})
	assert.NotNil(t, value, "TestRandResource failed")
	assert.Equal(t, uint64(1), value.meta.ID(), "TestRandResource failed")
}
