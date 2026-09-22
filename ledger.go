package dpengine

// ChargeKind 描述一次预算记账对应的组合语义。审计事件中始终记录该标记，
// 使每一笔消耗的累计口径可解释。
type ChargeKind string

const (
	// ChargeSequential：顺序组合口径，账本按求和累计。
	ChargeSequential ChargeKind = "sequential"
	// ChargeParallel：并行组合口径（互不相交分区），单次计入各分区
	// 参数的最大值（见 parallelCompose）。
	ChargeParallel ChargeKind = "parallel"
)

// Event 是一次不可变的预算消耗记录。Epsilon/Delta 记录该事件
// 对账本累计值的实际增量（并行查询时为各分区最大值）。
type Event struct {
	Seq       int64      `json:"seq"`
	RequestID string     `json:"request_id"`
	Kind      ChargeKind `json:"kind"`
	Epsilon   float64    `json:"epsilon"`
	Delta     float64    `json:"delta"`
	Detail    string     `json:"detail"`
}

// DatasetBudget 是单个数据集的预算与已消耗状态。
type DatasetBudget struct {
	Name          string  `json:"name"`
	EpsilonBudget float64 `json:"epsilon_budget"`
	DeltaBudget   float64 `json:"delta_budget"`
	EpsilonUsed   float64 `json:"epsilon_used"`
	DeltaUsed     float64 `json:"delta_used"`
	NextSeq       int64   `json:"next_seq,omitempty"`
	Events        []Event `json:"events"`
}

// Ledger 是按数据集名索引的内存账本。Requests 维护 requestID -> 数据集
// 的去重索引，保证重复请求不会重复计费。
type Ledger struct {
	Datasets map[string]*DatasetBudget `json:"datasets"`
	Requests map[string]string         `json:"requests"`
}

// NewLedger 创建空账本。
func NewLedger() *Ledger {
	return &Ledger{
		Datasets: map[string]*DatasetBudget{},
		Requests: map[string]string{},
	}
}

// Register 注册一个数据集并登记其总预算；同名重复注册被拒绝。
func (l *Ledger) Register(name string, budget PrivacyParams) error {
	if name == "" {
		return errorf(ErrInvalidParams, "dataset name must not be empty")
	}
	if _, exists := l.Datasets[name]; exists {
		return errorf(ErrDatasetExists, "dataset %q already registered", name)
	}
	l.Datasets[name] = &DatasetBudget{
		Name:          name,
		EpsilonBudget: budget.Epsilon,
		DeltaBudget:   budget.Delta,
		Events:        []Event{},
	}
	return nil
}

// Dataset 返回数据集账本及是否存在。
func (l *Ledger) Dataset(name string) (*DatasetBudget, bool) {
	db, ok := l.Datasets[name]
	return db, ok
}

// hasRequest 报告 requestID 是否已处理过，以及所属数据集。
func (l *Ledger) hasRequest(requestID string) (string, bool) {
	ds, ok := l.Requests[requestID]
	return ds, ok
}

// canCharge 判定在 kind 组合语义下追加 add 是否不超预算。
func (db *DatasetBudget) canCharge(add PrivacyParams) bool {
	return fitsBudget(db.EpsilonUsed, db.DeltaUsed, add.Epsilon, add.Delta,
		db.EpsilonBudget, db.DeltaBudget)
}

// charge 在已通过预算判定的前提下追加事件并累计预算，同时登记去重索引。
// 返回落账后的事件副本。调用方负责加锁与参数校验。
func (l *Ledger) charge(dataset string, kind ChargeKind, add PrivacyParams, requestID, detail string) Event {
	db := l.Datasets[dataset]
	db.NextSeq++
	ev := Event{
		Seq:       db.NextSeq,
		RequestID: requestID,
		Kind:      kind,
		Epsilon:   add.Epsilon,
		Delta:     add.Delta,
		Detail:    detail,
	}
	db.Events = append(db.Events, ev)
	db.EpsilonUsed += add.Epsilon
	db.DeltaUsed += add.Delta
	l.Requests[requestID] = dataset
	return ev
}
