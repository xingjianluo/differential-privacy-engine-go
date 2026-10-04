# differential-privacy-engine-go

一个本地差分隐私查询引擎：用本地账本（Ledger）维护每个数据集的隐私预算，
使噪声化结果与预算消耗在**并发、重放与重启**后都保持一致且可审计。
账本同时支持**先预占、后结算**的批次工作流（reserve / confirm / release /
过期回收），在高并发与长耗时任务之间也不会互相抢额度。
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

批量/长耗时任务在真正执行前，可先把整批可能用到的额度**预占（reserve）**
住：申请成功后这笔额度立即对其它请求（包括单次查询的 `Consume` 路径）
不可用；批次结束后按实际消耗**确认（confirm）**，或在取消/失败时
**释放（release）**未用部分。

```go
// 为批次 batch-1 在 users 数据集上预占 4.0 ε，持有上限 1 分钟
r, err := ledger.Reserve("users", "batch-1", 4.0, time.Minute)

// ... 批次执行（期间其它请求最多只能用 Total-Spent-4.0）...

if success {
    // 实际只用了 2.5：2.5 转为已用，剩余 1.5 立即回到可用池
    ledger.Confirm("batch-1", 2.5)
} else {
    // 整批取消/失败：全额释放，不计任何消耗
    ledger.Release("batch-1")
}
```

### 额度构成（`Budget`）

`ledger.Budget(dataset)` 返回同一时刻口径一致的五个量：

| 字段 | 含义 |
|---|---|
| `Total` | 数据集总预算 |
| `Spent` | 已结算消耗（单调不减，永不回落） |
| `Reserved` | **未到期**预占中、对他人真实不可用的额度 |
| `Releasable` | 已过截止时间但尚未被回收动作物化的过期持有额（`Expired` 是它的同义字段）；这笔额度**已计入** `Available`，在需要额度时会被自动收回 |
| `Expired` | 同 `Releasable`：已过期、等待（或即将被惰性）回收的部分 |
| `Available` | 当前立即可申请：`Total - Spent - Reserved`（只扣未到期占用，过期持有额不挡新请求） |

三个互斥类别恒等划分总预算：**`Total = Spent + Reserved + Available`**，
其中 `Releasable/Expired ⊆ Available`；任何路径、任何时刻都保持不变式
**`Spent + Reserved ≤ Total`**：直接扣费与新预占都会先把过期持有额收回、
再按未到期占用做检查，确认时 `actualEps` 不得超过预占额，因此总消耗永远
不会超过预算。

### 生命周期与“只生效一次”

```
reserve ──► reserved ──► confirmed   （actualEps 入 Spent，差额回 Available）
                    │
                    ├──► released    （到期前取消，全额释放，Spent 不变）
                    │
                    └──► expired     （到 ExpiresAt 即过期；由显式扫描或
                                       需要额度时的惰性回收物化，全额回池）
```

- **预占**：同一 `BatchID` 重复 `Reserve` 是幂等重放，返回当前状态，绝不
  二次占用；额度不足返回 `budget_exhausted`，数据集不存在返回
  `dataset_not_found`。
- **确认**：`actualEps` 必须满足 `0 ≤ actualEps ≤ ReservedEps`；超过预占
  被拒（`budget_exhausted`）且批次保持 `reserved`、占用不动。
- **释放/确认只生效一次**：批次进入终态后再次 confirm/release 一律返回
  `invalid_state`，额度不会被移动第二次；并发确认与并发释放（甚至
  confirm/release 混发）在互斥锁下只有一个操作生效。
- **批次不存在**的 confirm/release/查询返回 `batch_not_found`，与参数错误
  和状态错误明确区分。
- 每次 reserve/confirm/release/expire 都向批次审计轨迹追加一条
  `BatchEvent`（批次、数据集、目标状态、金额、时间），可通过
  `ledger.BatchHistory()` 获取，且该轨迹随快照持久化；无论过期是被扫描
  物化还是被惰性回收，都只有一条 expire 事件。

### 过期语义：截止时间是唯一判据

- 预占时通过 TTL 确定 `ExpiresAt = 申请时刻 + TTL`。**一旦当前时间
  `now ≥ ExpiresAt`，该预占在语义上就已经过期**——这与“有没有谁刚好跑过
  一次回收扫描”完全无关：
  - 过期之后再 `Confirm`（哪怕确认金额 ≤ 预占额）或 `Release` 一律失败，
    返回专用错误 `reservation_expired`，绝不会把实际消耗记进 `Spent`；
    该错误与 `batch_not_found`（批次不存在）、`invalid_state`（未过期但已
    结算）、`budget_exhausted`（未过期但超占）明确区分。
  - `GetReservation` 对过期批次返回 `expired` 状态；`Budget` 视图把
    过期未物化的持有额放在 `Releasable/Expired`，不计入 `Reserved`。
- **过期额度自动回收复用（无需任何后台动作）**：长期无人结算的预占不会
  把额度占死——新的 `Reserve` 或直接 `Consume` 在判定额度前，会先把相关
  数据集所有已到截止时间的持有额收回；因此“没人触发回收”时过期部分照样
  能被新请求使用。`ExpireReservations()` 作为**可选的显式扫描**仍然保留：
  它一次性物化所有到期预占（按 `BatchID` 排序、逐条追加过期事件），适合
  周期任务或在监控/对账时调用；重复扫描幂等，已结算批次永不被回收。
- **回收只生效一次**：显式扫描与惰性回收走的是同一段物化逻辑，只对仍为
  `reserved` 的批次生效。批次要么被结算（金额入 `Spent`、差额回池），
  要么被过期回收（全额回池），不可能既计实际消耗、又把同一笔额度放回池
  子；并发下“确认 vs 回收”相撞时，锁序决定谁先发生，但两者合计只有一次
  终态效果。
- 截止时刻即到期；时钟来自账本内部的 `now`（生产环境为 `time.Now`，测试
  可替换为固定时钟），因此过期行为完全可复现。重启不改变截止时间：恢复
  后 `now ≥ ExpiresAt` 的批次同样只按截止时间判定为过期，无需崩溃前跑过
  扫描。

## 预占状态的持久化与重启恢复

预占记录、批次状态与批次审计轨迹随快照一起持久化（快照版本 **v2**）：

- **未决预占不丢**：重启后处于 `reserved` 的批次原样恢复，占用额照旧计入
  账本合计，不会被重复占用（重放 `Reserve` 仍幂等），也不会被凭空释放；
  `ExpiresAt` 原样恢复，重启后继续受**同一条截止时间规则**约束——恢复时
  已过截止时间的批次立即按过期对待（拒绝 confirm/release 为
  `reservation_expired`、新申请需要额度时惰性收回），无需崩溃前曾扫描到
  点；尚未到期的扫描到点后照常回收。
- **不会结算两次**：终态（confirmed/released/expired）随快照保留，重启后
  再 confirm/release 仍按 `reservation_expired` / `invalid_state` 拒绝；
  过期物化（扫描或惰性）始终只有一次、只追加一条 expire 事件。
- **恢复即校验**：加载 v2 快照时交叉核对数据集占用合计与未决预占之和、
  `Spent + Reserved ≤ Total`、以及审计轨迹（每个批次恰有一条 reserve
  事件和至多一条终态事件，金额一致）；任何不一致都显式返回
  `corrupt_snapshot`，绝不带着矛盾状态运行。
- **旧账本安全加载**：v1/v0（无预占信息）快照按确定默认语义加载——预占
  集合为空、`Reserved = 0`、已有消耗与单次查询重放索引原样保留
  （`SpentEpsilon` 缺失时仍从审计轨迹末条推导），不会报错也不会清零；
  加载后可直接使用全部预占/结算能力。版本高于当前支持版本仍显式拒绝
  （`version_incompatible`）。

## 错误分类

所有失败返回 `*dpengine.Error`，`Reason` 字段可程序化区分：

| Reason | 含义 |
|---|---|
| `dataset_not_found` | 数据集未注册 |
| `invalid_parameter` | ε 非正数/NaN、请求 ID 为空、边界或模式非法；预占时 BatchID 空、ε 非法、TTL 非正；确认金额非法 |
| `budget_exhausted` | 本次消耗/预占将使 `Spent + Reserved` 超出总预算；确认金额超过预占额 |
| `unbounded_input` | 值越界（Reject 模式）或为 NaN/Inf |
| `version_incompatible` | 快照版本高于当前支持版本 |
| `corrupt_snapshot` | 快照无法解析或内部不一致（占用合计、状态机、审计轨迹对不上） |
| `batch_not_found` | confirm/release/get 指向不存在的批次 |
| `invalid_state` | 对**未到期但已结算**（confirmed/released）批次再次 confirm/release |
| `reservation_expired` | 批次已过 `ExpiresAt`：过期之后 confirm/release 一律拒绝（无论是否跑过回收扫描），与不存在、非法状态、超额明确区分 |

## 序列化与兼容范围

`SaveSnapshot(w, ledger)` / `LoadSnapshot(r)` 以带版本号的 JSON 持久化
账本（含消耗审计轨迹、幂等重放索引、预占记录与批次生命周期事件）。

- **恢复一致性**：恢复后的 `SpentEpsilon` 与崩溃前完全一致；崩溃前的
  请求 ID 在重启后重放仍不重复计费；新消耗从恢复状态继续单调累计；
  未决预占与其截止时间一并恢复，仍按同一条过期规则回收。
- **旧数据默认语义**：缺失 `SpentEpsilon` 的旧快照按确定规则从审计
  轨迹推导（取最后一条记录的 `SpentAfter`），重放索引同样从审计轨迹
  重建；v1/v0 无预占信息时默认空预占集、零占用，已有消耗不清零。
- **版本策略**：快照当前 `SnapshotVersion = 2`；版本 > 2 时**显式拒绝**
  （`version_incompatible`），绝不静默降级；v2 内部不一致或损坏快照
  显式拒绝（`corrupt_snapshot`）。

## 验证方法

```bash
go vet ./...
go test -race -count=1 ./...      # 全部测试（含竞态检测）
go test ./dpengine/ -v            # 查看每个用例的输入与判定依据日志
```

测试重点覆盖：

- **并发预算消耗**：64 worker × 10 请求对 ε=10 预算，断言恰好 100 次
  成功、`spent == budget`、零超额（`-race` 检测通过）。
- **预算耗尽边界**：剩余 1e-6 时 2e-6 被拒、1e-6 精确放行、拒绝后账本
  状态不变。
- **重复请求**：并发与串行重放同一 `RequestID` 均只计费一次。
- **预占即占位**：预占成功后，其它预占和单次查询直扣都被挡在
  `Total - Spent - Reserved` 之外；部分确认后未用差额立即回到可用池。
- **结算只生效一次**：64 个并发 confirm/release（含混发）恰好 1 个生效；
  重复确认、先确认后释放及其反向操作均返回 `invalid_state`。
- **并发预占不超额**：32 worker 各抢 0.5（名义共 16）对 ε=10，恰好
  20 笔成功、占用恰为 10；重复 BatchID 的并发预占只占用一次。
- **过期回收**：固定时钟下 t+30s 不回收、截止时刻精确回收、重复扫描
  幂等、已结算批次不被回收、回收额度可重新预占，并生成可审计事件。
- **截止时间为唯一过期判据**：不跑任何扫描，过期后的 confirm/release 同样
  被拒（`reservation_expired`），且与“先扫描后结算”分支的最终状态完全一致；
  到期前 1 秒仍可结算、到期时刻即拒；`batch_not_found`/`invalid_state`/
  `reservation_expired`/`budget_exhausted` 四类失败互不混淆。
- **无人回收也能复用过期额度**：不调用扫描时，新的预占与直接扣费都会在
  需要额度时惰性收回过期持有额并立即使用；`Budget` 视图把未到期占用
  （`Reserved`）与已过期待回收（`Releasable/Expired`）分开列示，且
  `Spent + Reserved ≤ Total` 恒成立。
- **并发撞车只生效一次**：8 轮 × 32 goroutine 的“截止前确认 vs 截止时
  回收”竞争、64 路重复 confirm/release（含混发），断言终态效果恰有一次、
  同一批次不会既计费又退额；全部在 `-race` 下通过。
- **重启后仍按截止时间判定**：保存时未到期、重启时已过期的批次无需任何
  扫描即被拒结算，其额度被新预占惰性复用，审计轨迹恰有一条 expire 事件，
  再次重启不重复回收。
- **重启恢复**：未决预占多轮快照往返后仍有效且只能结算一次、截止时间
  原样保留、恢复后仍能挡住抢占请求并按原规则过期回收；已结算批次、
  已用额度与审计事件顺序完整保留；v1/v0 旧快照默认加载、消耗不清零。
- **损坏 v2 快照**：占用合计不一致、超额、引用未知数据集/状态、重复
  批次、事件金额不一致、双重终态、缺失 reserve 事件等 8 类损坏一律
  `corrupt_snapshot` 拒绝。
- **错误归类**：所有非法输入路径断言到具体 `Reason`，测试日志打印
  输入与判定依据。
