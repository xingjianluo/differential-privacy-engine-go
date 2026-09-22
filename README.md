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

## 错误分类

所有失败返回 `*dpengine.Error`，`Reason` 字段可程序化区分：

| Reason | 含义 |
|---|---|
| `dataset_not_found` | 数据集未注册 |
| `invalid_parameter` | ε 非正数/NaN、请求 ID 为空、边界或模式非法 |
| `budget_exhausted` | 本次消耗将超出总预算 |
| `unbounded_input` | 值越界（Reject 模式）或为 NaN/Inf |
| `version_incompatible` | 快照版本高于当前支持版本 |
| `corrupt_snapshot` | 快照无法解析或内部不一致 |

## 序列化与兼容范围

`SaveSnapshot(w, ledger)` / `LoadSnapshot(r)` 以带版本号的 JSON 持久化
账本（含审计轨迹与幂等重放索引）。

- **恢复一致性**：恢复后的 `SpentEpsilon` 与崩溃前完全一致；崩溃前的
  请求 ID 在重启后重放仍不重复计费；新消耗从恢复状态继续单调累计。
- **旧数据默认语义**：缺失 `SpentEpsilon` 的旧快照按确定规则从审计
  轨迹推导（取最后一条记录的 `SpentAfter`），重放索引同样从审计轨迹
  重建。
- **版本策略**：快照版本 > 当前 `SnapshotVersion`（=1）时**显式拒绝**
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
- **错误归类**：所有非法输入路径断言到具体 `Reason`，测试日志打印
  输入与判定依据。
