# 本地差分隐私查询引擎（Go）

一个零外部依赖（仅 Go 标准库）的本地差分隐私（Local Differential Privacy）查询引擎。
引擎为每个数据集维护一份**本地预算账本**，所有噪声化结果与预算消耗在**并发、重放、
进程重启**后都保持确定一致，并且每一笔消耗都可审计、错误原因可区分。

- 模块路径：`github.com/xingjianluo/differential-privacy-engine-go`
- Go 版本：`go 1.25.0`（在 Go 1.26 工具链上构建验证）
- 无网络访问、无第三方依赖、无外部服务

---

## 1. 快速开始

```go
engine, err := dpengine.NewEngine(dpengine.WithStore("data/state.json"))
if err != nil { /* 版本不兼容或快照损坏 */ }
defer engine.Close()

// 1) 登记数据集总预算（epsilon 必须为正，delta ∈ [0,1)）
engine.RegisterDataset("users", dpengine.PrivacyParams{Epsilon: 1.0, Delta: 1e-6})

// 2) 带贡献上界的 Laplace 计数查询
res, err := engine.Count(dpengine.CountQuery{
    Dataset:          "users",
    RequestID:       "caller-generated-unique-id", // 幂等键
    Params:          dpengine.PrivacyParams{Epsilon: 0.5},
    MaxContributions: 3,                          // 每用户 L1 贡献上界
    Policy:          dpengine.PolicyClamp,         // 超界裁剪（另一种是整单拒绝）
    Contributions:   map[string]int{"alice": 3, "bob": 9},
})
// res.NoisyValue / res.TrueValue / res.ClampedContributions / res.NoiseScale
```

更多完整可运行示例见 [`example_test.go`](example_test.go)（`go test -run Example`）。

---

## 2. 噪声机制与贡献上界

### 2.1 Laplace 机制（满足 ε-差分隐私）

`Count` / `ParallelCount` 使用 Laplace 机制对**带符号整数计数求和**加噪：

- 每个用户的贡献被限制在 `[0, MaxContributions]`，相邻数据集（增/删一个用户）
  导致聚合真值的最大变化（L1 灵敏度）为 `Δ = MaxContributions`；
- 噪声尺度 `b = Δ / ε`，噪声 `Z ~ Lap(b)` 通过逆 CDF 采样：
  `Z = -b · sgn · ln(E)`，其中 `E ∈ (0,1]`，`sgn` 等概率取 ±1；
- 输出 `NoisyValue = TrueValue + Z` 满足 ε-差分隐私。查询声明的 `delta`
  仍按值记账（用于统一的 (ε, δ) 预算管理）；`delta=0` 即纯 ε-DP。

### 2.2 可复现性（固定随机源）

- 默认随机源由 `(requestID, "parallel"?, 分区索引)` 通过 FNV-1a 派生 128 位种子，
  再用 `math/rand/v2` 的 PCG 生成噪声。**相同请求在重放与重启后噪声逐位相同**，
  不同请求、不同分区的噪声流相互独立。
- 也可通过 `WithRandomSource(io.Reader)` 注入固定字节源（每次查询按序读取 16 字节），
  便于与外部确定性测试框架对接；读取失败返回 `internal_error` 且不记账。

### 2.3 无界输入与超界贡献：拒绝或裁剪，绝不静默

| 情形 | 行为 | 错误码 / 审计字段 |
| --- | --- | --- |
| `MaxContributions <= 0`（灵敏度无界） | 拒绝整单，不执行、不记账 | `unbounded_input` |
| 用户贡献为负 | 拒绝整单 | `invalid_params` |
| 贡献 > 上界且 `PolicyReject` | 拒绝整单，不记账 | `contribution_rejected` |
| 贡献 > 上界且 `PolicyClamp` | 裁剪到上界，结果中记录裁剪用户数 | `CountResult.ClampedContributions` |
| 隐私参数非法（ε≤0/NaN/±Inf，δ∉[0,1)） | 拒绝整单 | `invalid_params` |

裁剪路径的真值、灵敏度、噪声尺度与裁剪计数都在 `CountResult` 中返回，
结果始终可解释，不存在"静默返回不可解释结论"的路径。

---

## 3. 预算账本与组合规则

每个数据集持有独立预算 `(ε_budget, δ_budget)` 与只增的**事件日志**。
每个事件记录：单调递增序列号 `seq`、`request_id`、组合语义 `kind`、
本次实际入账的 `(ε, δ)` 与说明文字。可用 `Engine.Events(dataset)` 获取审计副本。

### 3.1 顺序组合（sequential）——默认规则

同一数据集上的多次查询按顺序组合累计：

```
ε_used = Σ ε_i        δ_used = Σ δ_i
```

### 3.2 并行组合（parallel）——互不相交分区

`ParallelCount` 在**同一数据集互不相交的分区**（每条记录只出现在一个子查询中，
由调用方负责保证）上执行多个子查询，整单只按一次并行组合计费：

```
ε_charge = max_i ε_i        δ_charge = max_i δ_i
```

审计事件标记为 `kind="parallel"`，记账金额为上述最大值。
并行请求整体只有一个 `request_id`，噪声种子额外混入分区索引保证独立。

### 3.3 放行判定与边界

- 放行条件：`used + charge <= budget + 1e-12`（固定绝对容差，消除浮点累计误差，
  恰好打满预算的最后一次请求合法，超预算的请求一律 `budget_exceeded`）；
- **超预算不记账、不出结果**：事件不追加、去重键不写入；
- 预算判定、机制执行、记账、落盘全部在单把互斥锁内完成；
- 并发场景下请求按到序串行裁决，因此 40 个并发请求竞争 10 个 ε=0.1 的名额时，
  恰好 10 个成功、30 个拿到 `budget_exceeded`，最终已用预算严格不超过 1.0。

### 3.4 幂等与去重（重复请求不重复计费）

- 每个 `request_id` 在账本的 `requests` 去重索引中只登记一次，并绑定所属数据集；
- 重复请求直接返回首次结果（快照中缺失结果缓存的旧数据则确定性重新生成），
  噪声与首次完全一致，预算不二次消耗；
- 同一 `request_id` 挪用到另一个数据集会被拒绝（`invalid_params`）。

### 3.5 提交顺序与故障回滚

成功路径严格按以下顺序提交，任何一步失败都整体回滚内存状态：

1. 校验参数、数据集存在性、预算；
2. 执行机制生成噪声；
3. 追加账本事件、累计预算、登记去重键；
4. 写入结果缓存；
5. **原子落盘**（临时文件 → fsync → rename → 目录 fsync）；
6. 第 5 步失败则撤销第 3、4 步，调用方可安全用同一 `request_id` 重试。

---

## 4. 序列化、恢复与版本兼容

### 4.1 快照格式

`WithStore(path)` 启用后，每次成功提交都写入缩进格式的 JSON 快照：

```json
{
  "format_version": 1,
  "ledger": {
    "datasets": {
      "users": {
        "name": "users",
        "epsilon_budget": 1.0,
        "delta_budget": 1e-6,
        "epsilon_used": 0.5,
        "delta_used": 0,
        "next_seq": 1,
        "events": [
          {"seq": 1, "request_id": "...", "kind": "sequential",
           "epsilon": 0.5, "delta": 0, "detail": "laplace count"}
        ]
      }
    },
    "requests": {"...": "users"}
  },
  "results": { "...": { "request_id": "...", "noisy_value": 0.0, "...": "..." } }
}
```

写入采用"同目录临时文件 + fsync + 原子 rename + 目录 fsync"，
崩溃时磁盘上要么是旧快照要么是新快照，不会出现半写文件。

### 4.2 兼容范围（明确，不静默降级）

| 快照情况 | 加载行为 |
| --- | --- |
| 文件不存在 | 视为全新空账本，正常启动 |
| 缺失 `format_version` 字段 | 按确定的历史默认（**版本 1**）加载 |
| `format_version == 1` 但缺失新字段 | 稳定默认：nil map 初始化；缺失的内联数据集名按键名恢复；**`epsilon_used/delta_used/next_seq` 一律从事件日志重放重建**，不相信快照中的汇总值 |
| 只有账本事件、缺失结果缓存的旧数据 | 正常加载；首次重放该请求时用同一 `request_id` 确定性重算结果并回填缓存 |
| `format_version < 1` 或为非法值 | 显式拒绝：`corrupt_state` |
| `format_version > 当前支持版本` | **显式拒绝：`incompatible_version`**，不做静默降级 |
| 事件缺 `request_id`/序列号非法/金额非法/已用超预算/同一请求跨数据集/结果缓存无对应事件 | 显式拒绝：`corrupt_state` |

恢复后预算状态与崩溃前一致（已用量从事件重放得到），序列号从事件最大值续写，
重复请求在重启后依然被识别。

---

## 5. 错误码（原因可区分）

所有错误均为 `*EngineError`，携带稳定的机读 `Code`，用 `dpengine.HasCode(err, code)`
判定，不要匹配错误文本。

| 错误码 | 触发条件 |
| --- | --- |
| `invalid_params` | ε/δ 非法、空数据集名/请求 ID、未知裁剪策略、并行子查询自带 ID、幂等键跨数据集复用 |
| `dataset_not_found` | 查询引用未注册数据集 |
| `dataset_exists` | 重复注册同名数据集（含重启后再注册恢复出的数据集） |
| `budget_exceeded` | 本次（顺序或并行组合后）消耗会超出数据集预算 |
| `unbounded_input` | 缺少正的每用户贡献上界，灵敏度无界 |
| `contribution_rejected` | 拒绝策略下存在超出上界（或负）的用户贡献 |
| `incompatible_version` | 快照格式版本高于引擎支持范围 |
| `corrupt_state` | 快照 JSON 损坏或语义一致性校验失败 |
| `internal_error` | 噪声尺度非法、外部随机源读取失败等内部故障（均不记账） |

---

## 6. 验证方法

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全部单测（每个用例都会在日志中打印输入与判定依据）
go test ./... -v -count=1

# 并发安全（竞态检测）
go test ./... -race -count=1

# 覆盖率
go test -cover ./...
```

测试重点（对应需求）：

- **并发预算消耗**：`TestConcurrentBudgetNoOverspend`（40 并发抢 10 名额，
  恰好放行 10、拒绝 30 且拒绝原因全部为 `budget_exceeded`、事件序列号连续）；
- **并发重复请求**：`TestConcurrentDuplicateRequest`（同一 `request_id` 50 个并发副本，
  全部拿到相同噪声结果、只记账一次）；
- **预算耗尽边界**：`TestEngineBudgetExhaustionBoundary`（0.4+0.4+0.2 恰好打满，
  之后任何正消耗都被拒且不留事件）；
- **重启恢复**：`TestRestartRestoresBudgetAndReplay`、
  `TestConcurrentBudgetThenRestart`（并发打满后重启，事件集合/预算/噪声完全一致，
  重启后旧请求重放不二次计费、新请求仍被拒）；
- **重复请求**：`TestEngineIdempotentDuplicate` 与上述并发/重启用例；
- **错误归类**：`TestEngineErrorClassification`、`TestInvalidQueryShapes`、
  `TestLoadFutureVersionRejected`、`TestLoadCorruptSnapshots` 等，
  逐类断言错误码并打印输入与判定依据；
- **机制正确性**：`TestLaplaceReproducible`（同种子同噪声、不同种子独立）、
  `TestLaplaceScaleAndMean`（大样本零均值）、拒绝/裁剪/无界输入路径；
- **提交原子性**：`TestSnapshotWriteFailureRollsBack`（快照写失败时注册/记账/缓存
  全部回滚，回滚后原 `request_id` 可重试）。

---

## 7. 包内 API 速览

| API | 说明 |
| --- | --- |
| `NewEngine(...Option)` | 创建引擎；`WithStore(path)` 启用快照，`WithRandomSource(r)` 注入随机源 |
| `RegisterDataset(name, PrivacyParams)` | 登记数据集总预算 |
| `Count(CountQuery)` | Laplace 计数（顺序组合口径），返回 `*CountResult` |
| `ParallelCount(ParallelCountQuery)` | 互不相交分区并行计数（按 max 组合计费） |
| `Used(dataset)` / `Budget(dataset)` | 查询已用预算 / 总预算与已用预算 |
| `Events(dataset)` | 获取审计事件副本 |
| `HasCode(err, ErrorCode)` | 判定错误码 |
| `LoadedFromSnapshot()` / `StorePath()` / `Close()` | 状态与生命周期 |
