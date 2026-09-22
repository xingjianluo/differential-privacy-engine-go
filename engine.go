package dpengine

import (
	"io"
	"strconv"
	"sync"
)

// Engine 是本地差分隐私查询引擎。
//
// 一致性设计要点：
//   - 单个 sync.RWMutex 串行化所有记账与查询，并发请求按到序依次判定，
//     超预算者被拒绝，不会出现超额放行或重复计费；
//   - 每个 requestID 只处理一次：重复请求直接返回首次缓存的结果，
//     重放（进程内）与重启（从快照恢复）行为一致；
//   - 噪声由 requestID 确定性派生（见 deriveSeed），固定输入下结果可复现；
//   - 成功路径严格按“判定预算 -> 执行机制 -> 记账 -> 原子落盘”的顺序提交：
//     落盘失败时整体回滚内存状态，调用方不会收到“结果已给但账本未记”
//     或“已计费但无结果”的状态。
type Engine struct {
	mu      sync.Mutex
	ledger  *Ledger
	results map[string]*CountResult
	store   string
	loaded  bool
	rng     io.Reader // 可选的外部随机源（测试注入）；非 nil 时用于噪声
}

// Option 配置 Engine。
type Option func(*Engine)

// WithStore 启用指定路径的 JSON 快照持久化（每次成功提交后原子落盘）。
func WithStore(path string) Option {
	return func(e *Engine) { e.store = path }
}

// WithRandomSource 注入固定随机字节源。读取协议与 crypto/rand 一致：
// 必须填满请求长度，否则视为随机源失败。与按 requestID 派生的确定性
// 噪声互斥：配置后噪声直接取自该源（每次请求按序读取 16 字节种子）。
func WithRandomSource(r io.Reader) Option {
	return func(e *Engine) { e.rng = r }
}

// NewEngine 创建引擎；配置快照时立即恢复，版本不兼容或数据损坏会返回错误。
func NewEngine(opts ...Option) (*Engine, error) {
	e := &Engine{
		ledger:  NewLedger(),
		results: map[string]*CountResult{},
	}
	for _, o := range opts {
		o(e)
	}
	ledger, results, loaded, err := loadState(e.store)
	if err != nil {
		return nil, err
	}
	e.ledger = ledger
	e.results = results
	e.loaded = loaded
	return e, nil
}

// RegisterDataset 注册数据集与其总预算。重启后恢复出的数据集无需也不能
// 重复注册（返回 ErrDatasetExists）。
func (e *Engine) RegisterDataset(name string, budget PrivacyParams) error {
	if err := budget.ValidateBudget(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ledger.Register(name, budget); err != nil {
		return err
	}
	if err := e.persistLocked(); err != nil {
		// 落盘失败：回滚注册，保持内存与磁盘一致。
		delete(e.ledger.Datasets, name)
		return err
	}
	return nil
}

// Count 执行一次顺序组合口径的带噪计数查询。
func (e *Engine) Count(q CountQuery) (*CountResult, error) {
	if err := validateCount(q); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	owner, seen := e.ledger.hasRequest(q.RequestID)
	if seen {
		if owner != q.Dataset {
			return nil, errorf(ErrInvalidParams,
				"request_id %q already used against dataset %q, cannot reuse for %q",
				q.RequestID, owner, q.Dataset)
		}
		if cached := e.results[q.RequestID]; cached != nil {
			return cached, nil
		}
		// 旧版本快照可能只含账本事件而无结果缓存：用当前请求参数
		// 确定性重新生成结果（噪声由 requestID 派生），不再记账。
		seed, err := e.seedFor(q.RequestID, "", 0)
		if err != nil {
			return nil, err
		}
		replayed, err := runCount(q, seed)
		if err != nil {
			return nil, err
		}
		// 回填结果缓存并落盘，使后续重放直接命中且快照保持完整。
		e.results[q.RequestID] = replayed
		if err := e.persistLocked(); err != nil {
			delete(e.results, q.RequestID)
			return nil, err
		}
		return replayed, nil
	}

	db, ok := e.ledger.Dataset(q.Dataset)
	if !ok {
		return nil, errorf(ErrDatasetNotFound,
			"dataset %q is not registered (request %q)", q.Dataset, q.RequestID)
	}
	if !db.canCharge(q.Params) {
		return nil, errorf(ErrBudgetExceeded,
			"request %q needs eps=%v delta=%v, dataset %q has eps=%v/%v delta=%v/%v used",
			q.RequestID, q.Params.Epsilon, q.Params.Delta, q.Dataset,
			db.EpsilonUsed, db.EpsilonBudget, db.DeltaUsed, db.DeltaBudget)
	}

	seed, err := e.seedFor(q.RequestID, "", 0)
	if err != nil {
		return nil, err
	}
	res, err := runCount(q, seed)
	if err != nil {
		return nil, err
	}

	// 记账 -> 缓存 -> 落盘；落盘失败整体回滚。
	ev := e.ledger.charge(q.Dataset, ChargeSequential, q.Params, q.RequestID,
		"laplace count")
	if cacheErr := e.cacheResultLocked(res); cacheErr != nil {
		e.rollbackLocked(q.Dataset, ev)
		return nil, cacheErr
	}
	if err := e.persistLocked(); err != nil {
		e.rollbackLocked(q.Dataset, ev)
		delete(e.results, q.RequestID)
		return nil, err
	}
	return res, nil
}

// ParallelCount 在同一数据集的互不相交分区上执行多个计数查询，
// 只按并行组合参数 max(eps_i), max(delta_i) 计费一次。
func (e *Engine) ParallelCount(pq ParallelCountQuery) ([]*CountResult, error) {
	if pq.RequestID == "" {
		return nil, errorf(ErrInvalidParams, "request_id must not be empty")
	}
	if len(pq.Children) == 0 {
		return nil, errorf(ErrInvalidParams,
			"parallel query %q must contain at least one child", pq.RequestID)
	}
	parts := make([]PrivacyParams, 0, len(pq.Children))
	for i := range pq.Children {
		// 并行请求整体只有一个幂等键；子查询不允许携带独立 requestID。
		// 空 ID 由引擎统一填充；与外层一致是重放同一请求的正常情形。
		if pq.Children[i].RequestID != "" && pq.Children[i].RequestID != pq.RequestID {
			return nil, errorf(ErrInvalidParams,
				"child %d of parallel request %q must not carry its own request_id",
				i, pq.RequestID)
		}
		pq.Children[i].Dataset = pq.Dataset
		pq.Children[i].RequestID = pq.RequestID
		if err := validateCount(pq.Children[i]); err != nil {
			return nil, err
		}
		parts = append(parts, pq.Children[i].Params)
	}
	charge := parallelCompose(parts)

	e.mu.Lock()
	defer e.mu.Unlock()

	owner, seen := e.ledger.hasRequest(pq.RequestID)
	if seen {
		if owner != pq.Dataset {
			return nil, errorf(ErrInvalidParams,
				"request_id %q already used against dataset %q, cannot reuse for %q",
				pq.RequestID, owner, pq.Dataset)
		}
		// 已处理过的并行请求：重放时不重复记账、不落盘，噪声由
		// requestID + 分区索引确定性重新生成，与首次结果一致。
		return e.executeParallelLocked(pq, charge, false)
	}

	db, ok := e.ledger.Dataset(pq.Dataset)
	if !ok {
		return nil, errorf(ErrDatasetNotFound,
			"dataset %q is not registered (parallel request %q)", pq.Dataset, pq.RequestID)
	}
	if !db.canCharge(charge) {
		return nil, errorf(ErrBudgetExceeded,
			"parallel request %q composed charge eps=%v delta=%v, dataset %q has eps=%v/%v delta=%v/%v used",
			pq.RequestID, charge.Epsilon, charge.Delta, pq.Dataset,
			db.EpsilonUsed, db.EpsilonBudget, db.DeltaUsed, db.DeltaBudget)
	}
	return e.executeParallelLocked(pq, charge, true)
}

// executeParallelLocked 执行（或确定性重放）所有分区并按并行口径提交一次。
func (e *Engine) executeParallelLocked(pq ParallelCountQuery, charge PrivacyParams, commit bool) ([]*CountResult, error) {
	results := make([]*CountResult, 0, len(pq.Children))
	for i, child := range pq.Children {
		seed, err := e.seedFor(pq.RequestID, "parallel", i)
		if err != nil {
			return nil, err
		}
		res, err := runCount(child, seed)
		if err != nil {
			return nil, err
		}
		res.Kind = string(ChargeParallel)
		res.Partition = partitionLabel(child, i)
		results = append(results, res)
	}
	if !commit {
		return results, nil // 重放：不再记账、不落盘
	}
	ev := e.ledger.charge(pq.Dataset, ChargeParallel, charge, pq.RequestID,
		"parallel laplace count across disjoint partitions")
	stub := &CountResult{
		RequestID:  pq.RequestID,
		Dataset:    pq.Dataset,
		Kind:       string(ChargeParallel),
		Partition:  "",
		TrueValue:  results[0].TrueValue,
		NoisyValue: results[0].NoisyValue,
		Policy:     results[0].Policy,
	}
	if cacheErr := e.cacheResultLocked(stub); cacheErr != nil {
		e.rollbackLocked(pq.Dataset, ev)
		return nil, cacheErr
	}
	if err := e.persistLocked(); err != nil {
		e.rollbackLocked(pq.Dataset, ev)
		delete(e.results, pq.RequestID)
		return nil, err
	}
	return results, nil
}

func partitionLabel(child CountQuery, i int) string {
	// 子查询显式给出分区名时用于审计；否则使用稳定序号标签。
	if name := child.Partition; name != "" {
		return name
	}
	return "partition-" + strconv.Itoa(i)
}

// cacheResultLocked 登记结果；同一 requestID 已有结果属于内部不变量破坏。
func (e *Engine) cacheResultLocked(res *CountResult) error {
	if _, exists := e.results[res.RequestID]; exists {
		return errorf(ErrInternal, "duplicate cached result for request %q", res.RequestID)
	}
	e.results[res.RequestID] = res
	return nil
}

// rollbackLocked 撤销最后一笔记账：移除事件、扣减预算、删除去重键。
// 仅用于同一次提交中缓存/落盘失败的回滚。
func (e *Engine) rollbackLocked(dataset string, ev Event) {
	db := e.ledger.Datasets[dataset]
	if db == nil {
		return
	}
	db.EpsilonUsed -= ev.Epsilon
	db.DeltaUsed -= ev.Delta
	if n := len(db.Events); n > 0 && db.Events[n-1].Seq == ev.Seq {
		db.Events = db.Events[:n-1]
		db.NextSeq = ev.Seq - 1
	}
	delete(e.ledger.Requests, ev.RequestID)
}

func (e *Engine) persistLocked() error { return saveState(e.store, e.ledger, e.results) }

// seedFor 为一次（子）查询确定 16 字节噪声种子：默认由 requestID 派生，
// 保证固定输入下可复现；配置外部随机源时改为从该源顺序读取。
func (e *Engine) seedFor(requestID, domain string, partition int) ([16]byte, error) {
	var zero [16]byte
	if e.rng == nil {
		return deriveSeed(requestID, domain, partition), nil
	}
	var seed [16]byte
	if _, err := io.ReadFull(e.rng, seed[:]); err != nil {
		return zero, wrapError(ErrInternal, err, "read random seed for request %q", requestID)
	}
	return seed, nil
}

// Used 返回数据集已消耗预算；ok=false 表示数据集不存在。
func (e *Engine) Used(dataset string) (epsilon, delta float64, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	db, exists := e.ledger.Dataset(dataset)
	if !exists {
		return 0, 0, false
	}
	return db.EpsilonUsed, db.DeltaUsed, true
}

// Budget 返回数据集总预算与已消耗预算；ok=false 表示数据集不存在。
func (e *Engine) Budget(dataset string) (totalEps, totalDelta, usedEps, usedDelta float64, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	db, exists := e.ledger.Dataset(dataset)
	if !exists {
		return 0, 0, 0, 0, false
	}
	return db.EpsilonBudget, db.DeltaBudget, db.EpsilonUsed, db.DeltaUsed, true
}

// Events 返回某数据集的审计事件副本（按序列号顺序）。
func (e *Engine) Events(dataset string) ([]Event, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	db, ok := e.ledger.Dataset(dataset)
	if !ok {
		return nil, false
	}
	out := make([]Event, len(db.Events))
	copy(out, db.Events)
	return out, true
}

// LoadedFromSnapshot 报告引擎是否从已有快照恢复。
func (e *Engine) LoadedFromSnapshot() bool { return e.loaded }

// StorePath 返回配置的快照路径（空串表示纯内存模式）。
func (e *Engine) StorePath() string { return e.store }

// Close 释放引擎资源；当前状态在每次提交后均已落盘，故 Close 无额外 IO。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ledger = nil
	e.results = nil
	return nil
}
