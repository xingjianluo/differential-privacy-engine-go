package dpengine

import "math"

// PrivacyParams 表示一次查询声明的 (epsilon, delta) 隐私参数。
//
// 预算组合语义（账本累计规则）：
//   - 顺序组合（同一数据集上的多次查询）：
//     eps_total = Σ eps_i，delta_total = Σ delta_i
//   - 并行组合（同一数据集上互不相交的分区上的查询，见
//     ChargeKindParallel 与 ParallelCountQuery）：
//     eps_total = max eps_i，delta_total = max delta_i
type PrivacyParams struct {
	Epsilon float64
	Delta   float64
}

// budgetTolerance 是预算比较的固定绝对容差，用于消除浮点累计误差，
// 避免最后一次合法请求被浮点误差误判为超额。
const budgetTolerance = 1e-12

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// Validate 校验隐私参数：epsilon 必须为正且有限；delta 必须位于 [0,1)。
// delta == 0 表示纯 epsilon-DP。
func (p PrivacyParams) Validate() error {
	if !isFinite(p.Epsilon) || p.Epsilon <= 0 {
		return errorf(ErrInvalidParams, "epsilon must be a finite positive number, got %v", p.Epsilon)
	}
	if !isFinite(p.Delta) || p.Delta < 0 || p.Delta >= 1 {
		return errorf(ErrInvalidParams, "delta must be a finite number in [0,1), got %v", p.Delta)
	}
	return nil
}

// ValidateBudget 校验数据集级预算：epsilon 预算必须为正；delta 预算位于 [0,1)。
func (p PrivacyParams) ValidateBudget() error {
	if !isFinite(p.Epsilon) || p.Epsilon <= 0 {
		return errorf(ErrInvalidParams, "epsilon budget must be a finite positive number, got %v", p.Epsilon)
	}
	if !isFinite(p.Delta) || p.Delta < 0 || p.Delta >= 1 {
		return errorf(ErrInvalidParams, "delta budget must be a finite number in [0,1), got %v", p.Delta)
	}
	return nil
}

// sequentialCompose 顺序组合：epsilon 与 delta 分别求和。
// 入参为空时返回零值（用于不记账的退化场景）。
func sequentialCompose(parts []PrivacyParams) PrivacyParams {
	var out PrivacyParams
	for _, p := range parts {
		out.Epsilon += p.Epsilon
		out.Delta += p.Delta
	}
	return out
}

// parallelCompose 并行组合：epsilon 与 delta 分别取最大值。
// 适用于输入互不相交分区上的子查询（每条记录只影响一个分区）。
func parallelCompose(parts []PrivacyParams) PrivacyParams {
	var out PrivacyParams
	for _, p := range parts {
		if p.Epsilon > out.Epsilon {
			out.Epsilon = p.Epsilon
		}
		if p.Delta > out.Delta {
			out.Delta = p.Delta
		}
	}
	return out
}

// fitsBudget 在固定容差下判定：已用预算 + 本次消耗是否不超过总预算。
func fitsBudget(usedEps, usedDelta, addEps, addDelta, budEps, budDelta float64) bool {
	if usedEps+addEps > budEps+budgetTolerance {
		return false
	}
	if usedDelta+addDelta > budDelta+budgetTolerance {
		return false
	}
	return true
}
