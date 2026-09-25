# go-consumer-groups

基于**递增分配版本（generation）**的消费组协调器：管理组成员、确定性分区分配、
会话超时与位点提交，并把协调状态持久化到磁盘。

开发环境：Go 1.23.0。

运行测试：

    go test ./...            # 单元与并发测试
    go test ./... -race      # 竞态检测

## 核心概念

### 分配版本（Generation）

- 组创建时版本为 0；每次成员变化（加入 / 主动离开 / 会话超时剔除）使版本 **+1**。
- 每个版本对应一份**完整、不可变**的分区分配快照（`Assignment`），再均衡时
  整体计算、整体替换发布——客户端要么读到整个旧版本，要么读到整个新版本，
  永远不会读到半套新分配。
- 所有变更在协调器内部同一把互斥锁下串行执行，因此成员离开、超时扫描与心跳
  无论以何种顺序并发到达，都只会形成**一条单调递增的版本序列**；被新版本
  淘汰的迟到操作在版本校验处即被拒绝，无法复活成员或覆盖新分配。

### 确定性分区分配

成员按 ID 字典序排序后，分区 `i` 固定归属 `members[i % len(members)]`：

- 同一版本内一个分区至多归属一个成员，成员间分区数差值不超过 1；
- 同样的成员集合永远产生同样的分配，不依赖加入顺序或 map 迭代顺序。

### 会话与心跳

- 成员周期发送心跳，心跳必须携带**当前**分配版本；携带旧版本（或超前版本）
  的心跳返回 `ErrIllegalGeneration`，已被剔除的成员返回 `ErrMemberNotFound`。
- 成员超过 `SessionTimeout` 未发送有效心跳，会被 `ExpireGroup` / `ExpireAll`
  超时扫描剔除并触发再均衡。

### 位点提交

- 只有**当前版本下该分区的所有者**能提交位点，否则返回 `ErrNotOwner`。
- 位点默认**单调递增**：提交更小的位点返回 `ErrOffsetBacktrack`；
  重置场景可显式设置 `AllowBacktrack`。
- **幂等请求号**（`RequestID`，必填）：
  - 同成员 + 同请求号 + 同分区 + 同位点 → 视为重放，原样成功（`Replayed=true`）；
  - 同成员 + 同请求号但分区或位点不同 → `ErrRequestConflict`；
  - 请求号的作用域是成员的一次在组生命周期，成员被剔除后记录随之清除。
- 并发提交在锁内串行执行：较大位点先落地时，较小的迟到提交被拒绝，
  因此**不会丢掉较大的合法值**。

### 持久化

`Store` 接口抽象状态持久化，每次状态变更后整体写入一份快照：

- `FileStore`：JSON 单文件，写入采用「临时文件 + fsync + 原子 rename」，
  读回时要么看到上一整份快照、要么看到新整份快照，不会读到半成品；
- `MemoryStore`：内存实现（默认），用于测试或无需跨进程持久化的场景。

进程重启后用同一 `Store` 重建 `Coordinator` 即可恢复：版本号、成员、分配、
位点与幂等记录全部还原，版本序列无缝延续。

## API 一览

```go
c, _ := consumergroups.NewCoordinator(consumergroups.NewFileStore("state.json"), nil)

// 组管理
c.CreateGroup(consumergroups.CreateGroupOptions{Name: "orders", Partitions: 8, SessionTimeout: 10 * time.Second})
st, _ := c.Status("orders")   // 完整状态快照：版本、成员、分配、位点
names := c.Groups()           // 所有组名

// 成员生命周期
join, _ := c.Join("orders", "consumer-1")        // 返回新版本与整份分配
hb, _ := c.Heartbeat("orders", "consumer-1", join.Generation)
c.Leave("orders", "consumer-1")
expired, _ := c.ExpireGroup("orders", time.Now()) // 或 c.ExpireAll(now) 周期扫描

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
| `ErrNotOwner` | `*NotPartitionOwnerError` | 提交者不是该分区当前所有者 |
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
| `coordinator.go` | 协调器：加入/心跳/离开/超时/提交/查询，锁内串行化 |
| `assign.go` | 确定性分区分配与 leader 选举 |
| `types.go` | 对外类型：`Assignment`、`GroupStatus`、`CommitRequest` 等 |
| `errors.go` | 哨兵错误与结构化错误类型 |
| `store.go` | `Store` 接口与快照模型 |
| `memory_store.go` / `file_store.go` | 内存 / 原子 JSON 文件持久化 |
