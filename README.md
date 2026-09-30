# differential-privacy-engine-go

一个本地差分隐私查询引擎：用本地账本（Ledger）维护每个数据集的隐私预算，
使噪声化结果与预算消耗在**并发、重放与重启**后都保持一致且可审计。
纯 Go 标准库实现（Go 1.25+），无任何外部依赖。

## 快速开始

```go
ledger := dpengine.NewLedger()
_ = ledger.AddDataset("users", 10.0) // 总预算 ε=10

engine := dpengine.NewEngine(dpengine.NewLaplaceMechanism(42 /* 固定种子可复现 */), ledger)

res, err := engine.Execute(dpengine.Query{
    Dataset:     "users",
    RequestID:   "req-001",              // 幂等键：重复请求不重复计费
    Epsilon:     0.5,
    Composition: dpengine.SequentialComposition,
    Values:      []float64{1.2, 3.4, 2.2},
    Lower:       0, Upper: 5,            // 贡献上界
    BoundMode:   dpengine.BoundClamp,    // 或 BoundReject
})
// res.NoisySum 为加噪后的求和结果；res.Consumption 为审计记录
```

## 噪声机制

实现 **Laplace 机制**（满足纯 ε-差分隐私）：对敏感度 Δ 的查询添加
`Laplace(0, Δ/ε)` 噪声，通过 CDF 逆变换采样
`X = -b·sign(U)·ln(1-2|U|)`，`U ~ Uniform(-1/2, 1/2)`。

- **贡献上界**：求和查询的每行贡献必须落在 `[Lower, Upper]` 内，
  敏感度为 `Upper - Lower`。`BoundClamp` 将越界值裁剪到界内（返回
  `ClampedRows` 计数），`BoundReject` 显式拒绝；`NaN`/`±Inf` 一律拒绝
  （`ReasonUnboundedInput`），绝不静默返回不可解释的结果。
- **可复现性**：随机源为 `math/rand/v2` 的 PCG，固定种子下噪声序列
  完全确定，相同输入产生相同输出。

## 预算组合规则

账本对每个数据集维护 `SpentEpsilon ≤ TotalEpsilon`，消耗**单调不减**，
每次消耗追加一条不可变的 `Consumption` 审计记录（请求 ID、ε、组合
规则、消耗后的累计值）。

| 组合规则 | 累计语义 |
|---|---|
| `SequentialComposition` | `spent += ε`（纯 DP 顺序组合） |
| `ParallelComposition` | `spent = max(spent, ε)`（不相交分区上的并行组合） |

- **幂等重放**：同一 `RequestID` 的重复请求返回首次记录，不重复计费。
- **并发安全**：检查-扣费在同一互斥锁内完成，并发请求不会超额放行；
  预算恰好耗尽边界的请求被精确拒绝，且不改变账本状态。
- **先验证后扣费**：参数校验与贡献裁剪失败时不消耗任何预算。

## 预算预占与批量结算

单次查询的“边执行边扣费”之外，账本支持**先预占、后结算**，用于在一批查询
真正执行前就把额度占住，避免高并发或长耗时任务之间互相抢额度：

```go
// 1. 为整批查询申请一笔额度：申请成功后立即对其它请求不可用
hold, err := ledger.Reserve("users", "batch-2026-09-30", 3.0, 10*time.Minute)

// 2a. 批次执行完，按实际消耗确认（actual ≤ reserved）；未用部分自动退回
settled, err := ledger.Confirm("batch-2026-09-30", 2.4)

// 2b. 或取消/失败：整笔预占原样退回，不计任何消耗
cancelled, err := ledger.Release("batch-2026-09-30")
```

状态机（每个转换在并发、重放、重启下**最多生效一次**）：

```
pending ──Confirm──▶ confirmed   实际消耗 actual（可为 0，≤ 预占额），
        │                          未用部分 (reserved-actual) 立即可用
        ├──Release──▶ released    整笔退回，不计消耗
        └──到达期限─▶ expired     由确定的过期回收规则结算，整笔退回
```

- **立即生效**：`Reserve` 成功后该笔额度马上计入预占，任何其它预占或直接
  扣费都会把它当作不可用；不变式恒为 `已用 + 预占中 ≤ 总预算`。
- **结算幂等**：重复 `Confirm` / `Release` 同一批次只返回首次结算结果
  （`Confirm` 已成功后 `Release` 反之亦然，返回 `invalid_state`），
  不会重复计费或凭空释放。`Confirm` 的实际消耗同时写入数据集审计轨迹，
  批次 ID 即幂等键（可像 `RequestID` 一样直接重放）。
- **额度视图**：`Budget(dataset)` 返回四种可区分额度——
  `Total`（总额）、`Spent`（已确认消耗）、`Reserved`（预占中、当前不可用）、
  `Releasable`（已过期待回收的预占，仍包含在 `Reserved` 内）、
  `Available`（当前可用 = Total − Spent − Reserved）。该只读接口**不改变状态**。

### 过期回收语义

预占时用 TTL 设定结算截止时间（`ExpiresAt = 预留时刻 + TTL`）。回收规则
确定且可审计：当某回收动作观察到 `now ≥ ExpiresAt` 时，pending 批次转为
`expired`、整笔额度退回可用池、不计消耗，并追加一条 `expired` 审计事件。

- `ReclaimExpired()` 对所有到期批次执行回收，返回被回收的批次清单供审计；
  重复扫描不会重复回收。
- `Reserve` 在检查新额度前，会先对该数据集做一次**惰性回收**，因此过期
  额度不会无限期阻塞新请求（即使没有外部定时扫描）。
- `Budget` 的 `Releasable` 字段在到期后、回收前即可观察到这笔“可释放”额度。
- 重启恢复的 pending 批次仍以其**原始截止时间**为准，不会被自动回收，也
  不会被重复占用；重启后由 `ReclaimExpired` / 下一次 `Reserve` 按同一规则结算。
- 截止之后才到达的 `Confirm` / `Release` 不再生效，批次按 `expired` 结算，
  返回 `invalid_state`，保证结算结果只取决于时间规则而不取决于调度时序。


## 错误分类

所有失败返回 `*dpengine.Error`，`Reason` 字段可程序化区分：

| Reason | 含义 |
|---|---|
| `dataset_not_found` | 数据集未注册 |
| `invalid_parameter` | ε 非正数/NaN、请求 ID 为空、边界或模式非法；或实际消耗超出预占额 |
| `budget_exhausted` | 本次消耗/预占将使 已用+预占 超出总预算 |
| `unbounded_input` | 值越界（Reject 模式）或为 NaN/Inf |
| `version_incompatible` | 快照版本高于当前支持版本 |
| `corrupt_snapshot` | 快照无法解析或内部不一致 |
| `batch_not_found` | 确认/释放/查询了不存在的批次 |
| `invalid_state` | 批次当前状态不允许该操作（已确认后释放、已释放后确认、已过期后结算） |

## 序列化与兼容范围

`SaveSnapshot(w, ledger)` / `LoadSnapshot(r)` 以带版本号的 JSON 持久化
账本（含审计轨迹与幂等重放索引）。

- **恢复一致性**：恢复后的 `SpentEpsilon` 与崩溃前完全一致；崩溃前的
  请求 ID 在重启后重放仍不重复计费；新消耗从恢复状态继续单调累计。
- **预占跨重启**：v2 快照额外保存全部批次（含 pending 与已结算批次的
  审计事件）。重启后 pending 预占**不丢失、不重复占用、不被凭空释放**，
  仍按原始截止时间参与过期回收；已确认批次（含实际消耗为 0 的结算）的
  审计记录随数据集一起恢复，重放仍只结算一次。加载时若快照暗示
  `已用 + 待结算预占 > 总预算`、批次引用了不存在的数据集、批次 ID 重复
  或状态非法，一律按 `corrupt_snapshot` 显式拒绝。
- **旧数据默认语义**：v0/v1 快照**没有预占信息**时按确定且安全的默认
  加载——已记录的消耗原样保留（缺失 `SpentEpsilon` 仍从审计轨迹推导），
  预占表从空开始；绝不因缺失预占字段报错，也不把已有消耗清零。加载后
  账本可立即正常发起预占。
- **版本策略**：快照版本 > 当前 `SnapshotVersion`（=2）时**显式拒绝**
  （`version_incompatible`），绝不静默降级；损坏快照显式拒绝
  （`corrupt_snapshot`）。

## 验证方法

```bash
go vet ./...
go test -race -count=1 ./...      # 全部测试（含竞态检测）
go test ./dpengine/ -v            # 查看每个用例的输入与判定依据日志
```

测试重点覆盖：

- **并发预算消耗**：64 worker × 10 请求对 ε=10 预算，断言恰好 100 次
  成功、`spent == budget`、零超额（`-race` 通过）。
- **预算耗尽边界**：剩余 1e-6 时 2e-6 被拒、1e-6 精确放行、拒绝后账本
  状态不变。
- **重复请求**：并发与串行重放同一 `RequestID` 均只计费一次。
- **重启恢复**：快照往返后预算一致、崩溃前请求重放不重复扣费、旧格式
  快照按默认语义加载、高版本快照显式拒绝。
- **预占结算**：预占立即占用额度（同时阻塞直接扣费与其它预占）、确认
  按实际消耗计费并退回余量、释放整笔退回、额度视图四分区可区分。
- **同批次并发确认/释放**：32 路并发重复确认只计费一次；32 路并发重复
  释放只退回一次；确认与释放竞争时**恰有一种结算结果**生效；32 worker
  并发“预占+确认”恰好结算 100 笔且 `已用+预占 ≤ 预算`（`-race` 通过）。
- **过期回收**：到期前不回收；到期瞬间进入 `Releasable` 但仍占用，
  `ReclaimExpired` 后额度退回且重复扫描无效；新预占触发惰性回收；
  过期后的确认/释放按 `expired` 结算且不计消耗。
- **未决预占重启恢复**：pending 预占跨快照保持占用、截止时间不变、
  重放 `Reserve` 不二次占用，并在原截止时间被回收；已确认批次（含
  actual=0）重启后重放不重复计费；v2 损坏快照逐项拒绝。
- **预算边界**：预占精确占满时放行、超 1e-9 拒绝且不改变持有额度。
- **错误归类**：所有非法输入路径断言到具体 `Reason`，测试日志打印
  输入与判定依据。
