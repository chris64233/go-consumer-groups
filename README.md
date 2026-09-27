# go-consumer-groups

基于**递增分配版本（generation）+ 协作式（cooperative）两阶段再均衡**的消费组
协调器：管理组成员、确定性分区分配、协作式分区移交、会话超时与位点提交，
并把全部协调状态持久化到磁盘。

开发环境：Go 1.23.0。

运行测试：

    go test ./...            # 单元与并发测试
    go test ./... -race      # 竞态检测

## 核心概念

### 分配版本（Generation）与单调状态机

- 组创建时版本为 0；每次成员变化（加入 / 主动离开 / 会话超时剔除）使版本 **+1**。
- 每个版本对应一份**完整、不可变的目标所有权**快照（`TargetAssignment`）。
- 版本状态机只有两个阶段，且只会沿一个方向推进：

  ```
  stable ──成员加入/离开/超时──▶ revoking ──全部旧所有者确认撤销──▶ stable
            generation += 1        （同一 generation 内收敛，不再 +1）
  ```

- 所有变更（心跳、离开、超时扫描、撤销确认、加入、提交）在协调器内部同一把
  互斥锁下串行执行，因此无论以何种顺序并发到达，都只会形成**一条单调的版本
  状态机序列**；携带旧版本的迟到操作在版本校验处即被拒绝，无法复活成员、
  无法提前完成再均衡，也无法覆盖新生效的所有权。

### 协作式再均衡（避免一次性撤销全部分区）

成员变化产生新版本时，协调器先算出**目标所有权**与每个旧所有者的**待撤销集合**，
分区按以下三类处理（见 `Join` / `Leave` / `ExpireGroup`）：

1. **未受影响分区**：新旧版本所有者相同，始终由原成员继续消费，完全不中断；
2. **无主分区**：旧版本无所有者（如新组、空组后重加入），新所有者**立即**取得；
3. **离开/超时成员的分区**：旧所有者已不在组内，由协调器**强制回收**并立即移交
   目标所有者，不需要它的撤销确认；
4. **两个存活成员之间转移的分区**：进入**待撤销（revoking）**状态——
   **旧所有者确认撤销之前，新所有者不能取得该分区**；此时生效所有权
   （`Assignment`）仍指向旧所有者，目标所有权（`TargetAssignment`）指向新所有者。

全体有撤销义务的成员都确认后，生效所有权追上目标所有权，版本在**同一个
generation 内**收敛为 `stable`（收敛本身不再增加版本号）。

### 撤销确认（AckRevocation）

`AckRevocation` 表示成员已停止消费指定分区、可以移交。确认必须**精确匹配**
组、成员、分配版本与分区集合四元组：

- 版本必须与协调器当前版本严格一致：旧版本 / 超前版本一律返回
  `ErrIllegalGeneration`，**不能提前完成整个再均衡**；
- 成员在当前版本必须确有撤销义务：稳定阶段的无关确认、或没有撤销义务的成员
  替别人确认，返回 `ErrNoRevocationInProgress`；
- 分区集合必须与该成员应撤销集合**精确相等**：漏项（少交）或额外分区（多交）
  返回 `ErrRevocationMismatch`（结构化错误中带 `Missing` / `Extra`），
  且本次**不转移任何分区**；
- 确认**可安全重试**：撤销阶段重复确认按幂等成功返回；再均衡随首次确认收敛后，
  迟到的重复确认也不报错（响应可能已在网络中丢失）。

确认一旦受理，对应分区的生效所有权立即从旧所有者切到目标所有者；成员可以在
`Heartbeat` 返回值的 `Revoking` 字段、或 `Status` 的 `PendingRevocations` /
`RevocationProgress` 中查到自己尚待撤销的分区。

### 确定性分区分配

成员按 ID 字典序排序后，分区 `i` 固定归属 `members[i % len(members)]`：

- 同一版本内一个分区至多归属一个成员，成员间分区数差值不超过 1；
- 同样的成员集合永远产生同样的目标所有权，不依赖加入顺序或 map 迭代顺序。

### 会话与心跳

- 成员周期发送心跳，心跳必须携带**当前**分配版本；携带旧版本（或超前版本）
  的心跳返回 `ErrIllegalGeneration`，已被剔除的成员返回 `ErrMemberNotFound`。
- 撤销阶段心跳同样被接受（成员一边继续消费未受影响分区、一边等待撤销确认），
  返回值带回当前生效所有权、目标所有权与该成员尚待撤销的分区；心跳不推进状态机。
- 成员超过 `SessionTimeout` 未发送有效心跳，会被 `ExpireGroup` / `ExpireAll`
  超时扫描剔除并触发再均衡；其名下分区（含尚待撤销的分区）被强制回收。

### 位点提交与所有权栅栏

- 提交只认分区的**当前生效所有者**，否则返回 `ErrNotOwner`。
- **待撤销分区在旧所有者确认撤销前仍归其有效持有**，因此它可以在移交前提交
  **最终位点**；未受影响分区全程可提交。
- 一旦确认受理、分区**转移完成**，旧成员的迟到提交（即使携带当前版本、即使是
  同一幂等请求号的重放）立即被**所有权栅栏**阻止（`ErrNotOwner`），无法在新所有
  者接手后覆盖位点；新所有者则可从最终位点之后继续提交。
- 位点默认**单调递增**：提交更小的位点返回 `ErrOffsetBacktrack`；
  重置场景可显式设置 `AllowBacktrack`。
- **幂等请求号**（`RequestID`，必填）：
  - 同成员 + 同请求号 + 同分区 + 同位点 → 视为重放，原样成功（`Replayed=true`）；
  - 同成员 + 同请求号但分区或位点不同 → `ErrRequestConflict`；
  - 请求号的作用域是成员的一次在组生命周期，成员被剔除后记录随之清除。
- 并发提交在锁内串行执行：较大位点先落地时，较小的迟到提交被拒绝，
  因此**不会丢掉较大的合法值**。

### 状态查询

`Status` 返回组的完整深拷贝快照，包含：

| 字段 | 含义 |
| --- | --- |
| `Generation` / `Phase` | 当前分配版本与阶段（`stable` / `revoking`） |
| `Assignment` | 当前**生效**所有权（未受影响分区保持原成员，待撤销分区仍是旧所有者） |
| `TargetAssignment` | 本版本的**目标**所有权；稳定时与 `Assignment` 一致 |
| `PendingRevocations` | 各成员**仍待确认**撤销的分区集合 |
| `RevocationProgress` | 各成员的应撤销（`Required`）/ 已确认（`Acked`）/ 是否完成（`Done`） |
| `Members` / `Leader` / `Offsets` | 成员、leader、已提交位点 |

### 持久化

`Store` 接口抽象状态持久化，每次状态变更后整体写入一份快照，快照内容包括：
版本号、阶段、成员、生效所有权、目标所有权、各成员撤销义务与确认进度、位点、
幂等记录。

- `FileStore`：JSON 单文件（快照格式版本 2），写入采用「临时文件 + fsync +
  原子 rename」，读回时要么看到上一整份快照、要么看到新整份快照，不会读到半成品；
- `MemoryStore`：内存实现（默认），用于测试或无需跨进程持久化的场景。

进程重启后用同一 `Store` 重建 `Coordinator` 即可恢复——**包括撤销阶段中途的
状态**：恢复后旧成员仍可继续确认撤销、版本在同一 generation 内收敛。加载时会
做跨字段一致性校验（阶段、生效/目标所有权、撤销义务、存活成员之间必须自洽），
损坏的快照直接报错而不是带病运行。

## 典型流程

```go
c, _ := consumergroups.NewCoordinator(consumergroups.NewFileStore("state.json"), nil)
c.CreateGroup(consumergroups.CreateGroupOptions{Name: "orders", Partitions: 8, SessionTimeout: 10 * time.Second})

// consumer-1 首次加入：无主分区立即取得，版本 stable
j1, _ := c.Join("orders", "consumer-1")

// consumer-2 加入：版本 +1 并进入 revoking；受影响分区等待 consumer-1 撤销
j2, _ := c.Join("orders", "consumer-2")
// j2.Assignment 是当前生效所有权（新分区可能仍归 consumer-1），
// j2.TargetAssignment 是最终目标。

// consumer-1 通过心跳或 Status 得知自己要撤销的分区，停掉这些分区的消费、
// 提交最终位点，然后确认撤销（确认可重试）：
hb, _ := c.Heartbeat("orders", "consumer-1", j2.Generation) // hb.Revoking 为待撤销列表
c.CommitOffset("orders", consumergroups.CommitRequest{ /* ... 最终位点 ... */ })
ack, _ := c.AckRevocation("orders", consumergroups.RevocationAckRequest{
    MemberID: "consumer-1", Generation: j2.Generation, Partitions: hb.Revoking,
})
// ack.CompletedRebalance == true 时版本在 j2.Generation 内收敛为 stable

// consumer-2 在下一次心跳中看到分区已生效归自己，开始消费
```

## API 一览

```go
// 组管理
c.CreateGroup(consumergroups.CreateGroupOptions{Name: "orders", Partitions: 8, SessionTimeout: 10 * time.Second})
st, _ := c.Status("orders")     // 完整状态：生效/目标所有权、待撤销集合、确认进度、位点
names := c.Groups()             // 所有组名

// 成员生命周期
join, _ := c.Join("orders", "consumer-1")                 // 新版本 + 生效/目标所有权
hb, _ := c.Heartbeat("orders", "consumer-1", join.Generation) // 返回阶段与待撤销分区
ack, _ := c.AckRevocation("orders", consumergroups.RevocationAckRequest{
    MemberID: "consumer-1", Generation: join.Generation, Partitions: hb.Revoking,
})
c.Leave("orders", "consumer-1")
expired, _ := c.ExpireGroup("orders", time.Now())          // 或 c.ExpireAll(now) 周期扫描

// 位点提交（只允许当前生效所有者）
res, err := c.CommitOffset("orders", consumergroups.CommitRequest{
    MemberID: "consumer-1", Generation: join.Generation,
    Partition: 0, Offset: 1024, RequestID: "commit-0001",
})
```

## 错误判别

所有错误都可用 `errors.Is` 分类，关键错误同时提供带上下文的结构化类型，
可用 `errors.As` 提取：

| 哨兵错误 | 结构化类型 | 含义 |
| --- | --- | --- |
| `ErrIllegalGeneration` | `*GenerationMismatchError` | 携带的分配版本与当前版本不一致（含 `Want`/`Got`）；旧版本心跳/提交/撤销确认一律拒绝 |
| `ErrNotOwner` | `*NotPartitionOwnerError` | 提交者不是该分区当前生效所有者（转移后旧主被栅栏） |
| `ErrRevocationMismatch` | `*RevocationMismatchError` | 撤销确认集合与应撤销集合不精确相等（含 `Expected`/`Missing`/`Extra`） |
| `ErrNoRevocationInProgress` | — | 稳定阶段或成员无撤销义务时的撤销确认 |
| `ErrRequestConflict` | `*RequestConflictError` | 同请求号改交了不同分区/位点 |
| `ErrOffsetBacktrack` | `*OffsetBacktrackError` | 位点后退且未显式允许 |
| `ErrGroupNotFound` / `ErrGroupAlreadyExists` | — | 组不存在 / 已存在 |
| `ErrMemberNotFound` / `ErrMemberAlreadyExists` | — | 成员不存在 / 已存在 |
| `ErrInvalidPartition` / `ErrInvalidArgument` | — | 分区越界 / 参数非法 |

```go
var rm *consumergroups.RevocationMismatchError
if errors.As(err, &rm) {
    // rm.Missing 为漏确认的分区，rm.Extra 为多确认的分区：客户端应按 Expected 重试
}
```

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `coordinator.go` | 协调器：版本状态机、加入/心跳/离开/超时/撤销确认/提交/查询，锁内串行化 |
| `assign.go` | 确定性分区分配与 leader 选举 |
| `types.go` | 对外类型：`Assignment`、`GroupStatus`、`RevocationAckRequest`、`CommitRequest` 等 |
| `errors.go` | 哨兵错误与结构化错误类型 |
| `store.go` | `Store` 接口、快照模型与恢复时的一致性校验 |
| `memory_store.go` / `file_store.go` | 内存 / 原子 JSON 文件持久化 |
