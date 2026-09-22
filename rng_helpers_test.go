package dpengine

import (
	"encoding/binary"
	"math/rand/v2"
)

// newPCGRand 是测试辅助：从 16 字节确定性种子构造 PCG Rand。
func newPCGRand(seed [16]byte) *rand.Rand {
	return rand.New(rand.NewPCG(
		binary.BigEndian.Uint64(seed[:8]),
		binary.BigEndian.Uint64(seed[8:]),
	))
}
