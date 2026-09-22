package dpengine

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/rand/v2"
)

// BoundPolicy 决定用户贡献超出 MaxContributions 上界时的处理路径。
// 查询必须显式给出上界与策略，不存在静默放行的无界路径。
type BoundPolicy int

const (
	// PolicyReject：任一用户贡献为负或超过上界即拒绝整个查询，不记账、不出结果。
	PolicyReject BoundPolicy = iota
	// PolicyClamp：将每个用户贡献裁剪到 [0, MaxContributions]，
	// 被裁剪的用户数计入 CountResult.ClampedContributions 供审计。
	PolicyClamp
)

// CountQuery 是基于 Laplace 机制的带贡献上界的计数（求和）查询。
//
// 每个用户可贡献 [0, MaxContributions] 范围内的整数；因此聚合的
// L1 灵敏度为 MaxContributions，噪声尺度 b = MaxContributions/epsilon。
// 该机制满足 epsilon-差分隐私（预算中的 delta 仍按声明值记账）。
type CountQuery struct {
	Dataset          string         // 目标数据集
	RequestID        string         // 调用方生成的幂等键（并行子查询必须留空，由引擎填充）
	Partition        string         // 可选：并行组合下的分区名，仅用于审计标识
	Params           PrivacyParams  // 本次声明的隐私参数
	MaxContributions int            // 每用户贡献上界（L1 灵敏度），必须 > 0
	Policy           BoundPolicy    // 超界处理：拒绝或裁剪
	Contributions    map[string]int // 用户 -> 原始贡献；缺失用户视为 0
}

// CountResult 是计数查询的噪声化结果与审计字段。
type CountResult struct {
	RequestID            string  `json:"request_id"`
	Dataset              string  `json:"dataset"`
	Kind                 string  `json:"kind"` // "sequential" / "parallel"
	Partition            string  `json:"partition,omitempty"`
	TrueValue            int64   `json:"true_value"`
	NoisyValue           float64 `json:"noisy_value"`
	Sensitivity          float64 `json:"sensitivity"`
	NoiseScale           float64 `json:"noise_scale"`
	ClampedContributions int     `json:"clamped_contributions"`
	Policy               string  `json:"policy"`
}

// ParallelCountQuery 在同一数据集的互不相交分区上执行多个 CountQuery。
// 调用方负责保证分区不相交（每条用户记录只出现在一个子查询中）；
// 引擎只对并行组合参数 max(eps), max(delta) 计费一次。
type ParallelCountQuery struct {
	Dataset   string
	RequestID string
	Children  []CountQuery // 各子查询的 Dataset 字段被忽略，统一用外层 Dataset
}

// deriveSeed 由 (requestID, 域标签, 分区索引) 确定性派生 128 位随机种子。
// 同一逻辑请求在重放与重启后得到完全相同的噪声，且不同分区噪声相互独立。
func deriveSeed(requestID, domain string, partition int) [16]byte {
	var buf [16]byte
	h1 := fnv.New64a()
	h1.Write([]byte(domain))
	h1.Write([]byte{0})
	h1.Write([]byte(requestID))
	var ib [8]byte
	binary.BigEndian.PutUint64(ib[:], uint64(partition))
	h1.Write(ib[:])
	binary.BigEndian.PutUint64(buf[:8], h1.Sum64())

	h2 := fnv.New64a()
	h2.Write([]byte("dpengine:noise:v2"))
	h2.Write([]byte{0})
	h2.Write([]byte(requestID))
	binary.BigEndian.PutUint64(ib[:], ^uint64(partition))
	h2.Write(ib[:])
	binary.BigEndian.PutUint64(buf[8:], h2.Sum64())
	return buf
}

// laplaceNoise 生成尺度为 scale 的 Laplace 噪声：-scale*sgn*ln(E)，
// E 取自 (0,1]，sgn 等概率取 ±1。scale 必须为正有限值。
func laplaceNoise(rng *rand.Rand, scale float64) (float64, error) {
	if !isFinite(scale) || scale <= 0 {
		return 0, errorf(ErrInternal, "laplace scale must be finite and positive, got %v", scale)
	}
	u := float64(rng.Uint64()) / (float64(math.MaxUint64) + 1) // [0,1)
	e := 1 - u                                                 // (0,1]
	mag := -scale * math.Log(e)
	if rng.Uint64()&1 == 1 {
		mag = -mag
	}
	return mag, nil
}

// validateCount 校验计数查询的结构性参数（不含预算与数据集存在性）。
func validateCount(q CountQuery) error {
	if q.RequestID == "" {
		return errorf(ErrInvalidParams, "request_id must not be empty")
	}
	if q.MaxContributions <= 0 {
		// 缺少有限贡献上界：无界输入必须被显式拒绝。
		return errorf(ErrUnboundedInput,
			"max_contributions must be > 0 to bound per-user sensitivity (request %q)", q.RequestID)
	}
	switch q.Policy {
	case PolicyReject, PolicyClamp:
	default:
		return errorf(ErrInvalidParams, "unknown bound policy %d (request %q)", q.Policy, q.RequestID)
	}
	return q.Params.Validate()
}

// boundedCount 应用贡献上界：
//   - PolicyReject：存在负数或超上界贡献时返回 ErrContributionRejected；
//   - PolicyClamp：裁剪到 [0, MaxContributions] 并统计被裁剪用户数。
func boundedCount(q CountQuery) (trueValue int64, clamped int, err error) {
	for user, c := range q.Contributions {
		switch {
		case c < 0:
			return 0, 0, errorf(ErrInvalidParams,
				"negative contribution %d from user %q is not allowed (request %q)", c, user, q.RequestID)
		case c > q.MaxContributions:
			if q.Policy == PolicyReject {
				return 0, 0, errorf(ErrContributionRejected,
					"user %q contribution %d exceeds max %d under reject policy (request %q)",
					user, c, q.MaxContributions, q.RequestID)
			}
			c = q.MaxContributions
			clamped++
		}
		trueValue += int64(c)
	}
	return trueValue, clamped, nil
}

// runCount 是单个计数查询的机制执行：校验 -> 上界处理 -> Laplace 加噪。
// 不触碰账本。rngSeed 决定噪声；结果由其完全确定。
func runCount(q CountQuery, rngSeed [16]byte) (*CountResult, error) {
	if err := validateCount(q); err != nil {
		return nil, err
	}
	trueValue, clamped, err := boundedCount(q)
	if err != nil {
		return nil, err
	}
	scale := float64(q.MaxContributions) / q.Params.Epsilon
	rng := rand.New(rand.NewPCG(
		binary.BigEndian.Uint64(rngSeed[:8]),
		binary.BigEndian.Uint64(rngSeed[8:]),
	))
	noise, err := laplaceNoise(rng, scale)
	if err != nil {
		return nil, err
	}
	policyName := "clamp"
	if q.Policy == PolicyReject {
		policyName = "reject"
	}
	return &CountResult{
		RequestID:            q.RequestID,
		Dataset:              q.Dataset,
		Kind:                 string(ChargeSequential),
		Partition:            q.Partition,
		TrueValue:            trueValue,
		NoisyValue:           float64(trueValue) + noise,
		Sensitivity:          float64(q.MaxContributions),
		NoiseScale:           scale,
		ClampedContributions: clamped,
		Policy:               policyName,
	}, nil
}
