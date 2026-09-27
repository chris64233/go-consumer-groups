# go-consumer-groups

基于**递增分配版本（generation）**的消费组协调器：管理组成员、确定性分区分配、
**协作式再均衡**、会话超时与位点提交，并把协调状态持久化到磁盘。

开发环境：Go 1.23.0。

运行测试：

    go test ./...            # 单元与并发测试
    go test ./... -race      # 竞态检测

## 核心概念

### 分配版本（Generation）

- 组创建时版本为 0；每次成员变化（加入 / 主动离开 / 会话超时剔除）使版本 **+1**。
- 每个版本对应一份**完整、不可变**的目标分配快照，再均衡时整体计算、整体发布——
  客户端要么读到整个旧版本，要么读到整个新版本，永远不会读到半套新分配。
- 所有变更在协调器内部同一把互斥锁下串行执行，因此成员离开、超时扫描、心跳与
  撤销确认无论以何种顺序并发到达，都只会形成**一条单调递增的版本序列**；
  被新版本淘汰的迟到操作在版本校验处即被拒绝，无法复活成员或覆盖新分配。

### 协作式再均衡（Cooperative Rebalance）

成员变化时**不再一次性撤销全部分区**，而是分阶段推进：

1. **生成目标**：新版本生成目标所有权（`Target`），与当前有效所有权
   （`Assignment`）的差异构成**待撤销集合**（`PendingRevocations`）。
2. **继续消费**：未受影响的分区继续由原成员消费；待撤销分区在确认前也仍归
   旧所有者——新所有者**不能提前取得**。
3. **确认撤销**：旧所有者停止消费待撤销分区、提交最终位点后，调用
   `AcknowledgeRevocation` 确认；确认生效时分区所有权立即转移给目标所有者。
4. **强制回收**：成员在撤销阶段主动离开或会话超时时，其剩余待撤销分区由
   协调器强制回收，立即转移，不阻塞整组进度。

撤销确认的约束：

- 确认必须**精确匹配**组、成员、分配版本与分区集合四要素；
- 分区集合**漏报或多报**都返回 `ErrRevocationMismatch`（携带 `Missing`/`Extra`
  明细），不会部分生效，也不会提前完成整个再均衡；
- **旧版本确认**返回 `ErrIllegalGeneration`；
- 确认是**幂等**的：相同内容重试原样成功，并发重复确认只生效一次。

### 确定性分区分配

成员按 ID 字典序排序后，分区 `i` 固定归属 `members[i % len(members)]`：

- 同一版本内一个分区至多归属一个成员，成员间分区数差值不超过 1；
- 同样的成员集合永远产生同样的目标分配，不依赖加入顺序或 map 迭代顺序。

### 会话与心跳

- 成员周期发送心跳，心跳必须携带**当前**分配版本；携带旧版本（或超前版本）
  的心跳返回 `ErrIllegalGeneration`，已被剔除的成员返回 `ErrMemberNotFound`。
- 心跳与加入的返回值携带目标所有权与本成员**待撤销分区列表**
  （`HeartbeatResult.Revocations`），客户端据此驱动撤销确认。
- 成员超过 `SessionTimeout` 未发送有效心跳，会被 `ExpireGroup` / `ExpireAll`
  超时扫描剔除并触发再均衡；其待撤销分区被强制回收。

### 位点提交

- 只有**当前有效所有者**能提交位点，否则返回 `ErrNotOwner`。
- 处于**待撤销**状态的分区，旧所有者可在确认撤销前提交**最终位点**；
  一旦转移完成，旧成员的迟到提交即被栅栏（`ErrNotOwner`），携带旧版本的
  提交在版本校验处被拒绝（`ErrIllegalGeneration`）。
- 位点默认**单调递增**：提交更小的位点返回 `ErrOffsetBacktrack`；
  重置场景可显式设置 `AllowBacktrack`。
- **幂等请求号**（`RequestID`，必填）：
  - 同成员 + 同请求号 + 同分区 + 同位点 → 视为重放，原样成功（`Replayed=true`）；
  - 同成员 + 同请求号但分区或位点不同 → `ErrRequestConflict`；
  - 请求号的作用域是成员的一次在组生命周期，成员被剔除后记录随之清除。
- 并发提交在锁内串行执行：较大位点先落地时，较小的迟到提交被拒绝，
  因此**不会丢掉较大的合法值**。

### 状态查询

`Status` 返回组的完整状态快照，包括协作式再均衡的全部协调状态：

- `Assignment`：当前有效所有权；
- `Target`：本代目标所有权；
- `PendingRevocations`：待撤销集合（分区、旧所有者、目标所有者）；
- `AckProgress`：各成员确认进度（应撤销 / 已确认 / 待确认 / 被强制回收）；
- `RebalanceComplete`：本代再均衡是否已完成。

### 持久化

`Store` 接口抽象状态持久化，每次状态变更后整体写入一份快照，
快照涵盖版本、成员、有效/目标分配、待撤销集合、确认进度、位点与幂等记录：

- `FileStore`：JSON 单文件，写入采用「临时文件 + fsync + 原子 rename」，
  读回时要么看到上一整份快照、要么看到新整份快照，不会读到半成品；
  格式版本 v2，旧版 v1 快照（无协作式字段）按「再均衡已完成」兼容加载；
- `MemoryStore`：内存实现（默认），用于测试或无需跨进程持久化的场景。

进程重启后用同一 `Store` 重建 `Coordinator` 即可恢复：再均衡中途的状态
（待撤销集合、各成员确认进度）同样还原，版本序列无缝延续。

## API 一览

```go
c, _ := consumergroups.NewCoordinator(consumergroups.NewFileStore("state.json"), nil)

// 组管理
c.CreateGroup(consumergroups.CreateGroupOptions{Name: "orders", Partitions: 8, SessionTimeout: 10 * time.Second})
st, _ := c.Status("orders")   // 完整状态快照：版本、成员、有效/目标分配、待撤销集合、确认进度、位点
names := c.Groups()           // 所有组名

// 成员生命周期
join, _ := c.Join("orders", "consumer-1")        // 返回新版本、有效/目标分配与本成员待撤销分区
hb, _ := c.Heartbeat("orders", "consumer-1", join.Generation)
c.Leave("orders", "consumer-1")
expired, _ := c.ExpireGroup("orders", time.Now()) // 或 c.ExpireAll(now) 周期扫描

// 协作式再均衡：确认撤销（心跳返回值会告知本成员待撤销分区）
ack, _ := c.AcknowledgeRevocation("orders", consumergroups.RevocationAck{
    MemberID: "consumer-1", Generation: hb.Generation, Partitions: hb.Revocations,
})
// ack.RebalanceComplete 为 true 表示本代再均衡全部完成

// 位点提交
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
| `ErrIllegalGeneration` | `*GenerationMismatchError` | 携带的分配版本与当前版本不一致（含 `Want`/`Got`） |
| `ErrNotOwner` | `*NotPartitionOwnerError` | 提交者不是该分区当前有效所有者 |
| `ErrRevocationMismatch` | `*RevocationMismatchError` | 撤销确认集合与待撤销集合不一致（含 `Missing`/`Extra`） |
| `ErrRequestConflict` | `*RequestConflictError` | 同请求号改交了不同分区/位点 |
| `ErrOffsetBacktrack` | `*OffsetBacktrackError` | 位点后退且未显式允许 |
| `ErrGroupNotFound` / `ErrGroupAlreadyExists` | — | 组不存在 / 已存在 |
| `ErrMemberNotFound` / `ErrMemberAlreadyExists` | — | 成员不存在 / 已存在 |
| `ErrInvalidPartition` / `ErrInvalidArgument` | — | 分区越界 / 参数非法 |

```go
var gm *consumergroups.GenerationMismatchError
if errors.As(err, &gm) {
    // gm.Want 是协调器当前版本，gm.Got 是请求携带的版本：客户端应重新同步分配
}
```

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `coordinator.go` | 协调器：加入/心跳/离开/超时/撤销确认/提交/查询，锁内串行化 |
| `assign.go` | 确定性分区分配与 leader 选举 |
| `types.go` | 对外类型：`Assignment`、`RevocationAck`、`GroupStatus`、`CommitRequest` 等 |
| `errors.go` | 哨兵错误与结构化错误类型 |
| `store.go` | `Store` 接口与快照模型（含协作式再均衡状态） |
| `memory_store.go` / `file_store.go` | 内存 / 原子 JSON 文件持久化 |

