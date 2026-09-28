# go-consumer-groups

基于**递增分配版本（generation）+ 协作式（cooperative）两阶段再均衡**的消费组
协调器：管理组成员、**静态成员身份（static membership）与会话版本栅栏**、
确定性分区分配、协作式分区移交、两级会话超时与位点提交，
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

### 静态成员：稳定实例标识、会话版本与重连

普通成员（`Join`）断线即被彻底剔除。**静态成员**用 `JoinStatic` 以一个稳定的
**实例标识** `InstanceID` 加入，用于「短暂重连不必转移全部分区」的场景：

- **会话与版本**：每次成功加入分配一个单调递增的 **会话版本**
  （`SessionVersion`，首次为 1），并返回本次**会话 ID**（`SessionID`，通常是
  进程/容器实例 ID）。后续心跳、撤销确认、位点提交都必须携带
  `(SessionID, SessionVersion)`。分区所有权挂在稳定的 `InstanceID` 上，与具体
  会话无关。
- **暂时离线不转移分区**：静态会话超过 `SessionTimeout` 未心跳时，实例转为
  **暂时离线**，但在保留期 `Retention` 内**仍占有全部分区**——此过程**不推进
  分配版本、不触发再均衡、也不安排新所有者**；协调器据此区分「暂时离线」与
  「真正退出」。
- **保留期内重连**：用相同 `InstanceID`（新的 `SessionID`）再次 `JoinStatic`，
  会话版本 +1、返回 `Rejoined=true`，**继续原分配、不触发再均衡**。
- **旧进程栅栏**：被更高会话版本接管后，旧进程随后发来的**心跳、撤销确认、
  位点提交一律被拒绝**（`ErrFencedSession`）——它既不能确认新版本的撤销集合，
  也不能让位点倒退。即使新旧进程使用相同会话 ID，版本递增仍能栅栏旧进程。

#### 什么时候才真正清退静态实例

只有以下三种情况，静态实例才真正退出并把分区**纳入协作式再均衡**：

1. **超过保留期**仍未以更高会话版本重连（`ExpireGroup` / `ExpireAll` 扫描到
   `now > OfflineAt + Retention`）；
2. **主动退出** `LeaveStatic`；
3. **被管理员移除** `RemoveInstance`。

清退时实例原持有的分区被协调器**强制回收**（不需要撤销确认），并作为**一整批
只移交给唯一的新所有者**（在线成员中确定性选出的环形后继），不会被拆散给多个
成员；没有其他在线成员时这些分区回到无主。

> `ExpireGroup` 的返回值只包含**真正退出**的成员/实例；仅从在线转为暂时离线的
> 静态会话不计入返回值（也不推进版本），但其离线时间与保留期限会被持久化。

#### 撤销进行中断线

若静态实例在某个版本的撤销过程中断线（还没确认撤销）：它的**撤销义务挂起**——
分区仍由该离线实例生效持有，其他成员的确认照常，但版本不会越过这些分区收敛。
保留期内重连后由**新会话**（携带新会话版本）确认挂起的撤销集合，版本在**同一
generation 内**收敛；若保留期届满仍未重连，则这些分区被强制回收。

#### 动态成员与静态成员的区别

| | 动态成员 `Join` | 静态成员 `JoinStatic` |
| --- | --- | --- |
| 标识 | 成员 ID | 稳定实例 ID + 每次会话 ID |
| 会话版本 | 无 | 每次加入单调 +1 |
| 会话超时后 | 立即剔除并再均衡 | 保留期内仅暂时离线，分区不动 |
| 真正退出 | 超时 / `Leave` | 保留期届满 / `LeaveStatic` / `RemoveInstance` |
| 幂等请求记录作用域 | 一次在组生命周期 | 实例本身（跨重连/接管持续生效） |

静态会话不能调用动态 `Leave`（返回 `ErrInvalidArgument`），反之亦然。

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
  - 同实例 + 同请求号 + 同分区 + 同位点 → 视为重放，原样成功（`Replayed=true`）；
  - 同实例 + 同请求号但分区或位点不同 → `ErrRequestConflict`；
  - 动态成员的请求号作用域是一次在组生命周期（被剔除后清除）；**静态成员的请求号
    归属于实例本身，跨重连/接管持续生效**，因此新会话与旧会话的最终提交在同一条
    单调序列上比较。
- 并发提交在锁内串行执行：较大位点先落地时，较小的迟到提交被拒绝，
  因此**不会丢掉较大的合法值**。

### 状态查询

`Status` 返回组的完整深拷贝快照，包含：

| 字段 | 含义 |
| --- | --- |
| `Generation` / `Phase` | 当前分配版本与阶段（`stable` / `revoking`） |
| `Members` | 当前**在线会话**：动态成员与在线静态实例的当前会话（含会话版本、实例标识） |
| `StaticInstances` | 全部静态实例（**含保留期内暂时离线者**）：实例身份、在线状态、当前/最后会话与版本、离线时间、保留期限、生效持有分区 |
| `Assignment` | 当前**生效**所有权（未受影响分区保持原成员，待撤销分区仍是旧所有者） |
| `TargetAssignment` | 本版本的**目标**所有权；稳定时与 `Assignment` 一致 |
| `PendingRevocations` | 各成员**仍待确认**撤销的分区集合（按所有权主体：动态成员 ID 或静态实例 ID） |
| `RevocationProgress` | 各成员的应撤销（`Required`）/ 已确认（`Acked`）/ 是否完成（`Done`） |
| `PendingTransfers` | 尚未最终落地、等待转移的分区：`revoking`（在线旧主待确认）、`suspended`（旧实例保留期内离线、撤销义务挂起）、`retained`（离线实例钉住、尚无新所有者），含当前/目标所有者、在线状态、实例与会话版本 |
| `Leader` / `Offsets` | leader、已提交位点 |

### 持久化

`Store` 接口抽象状态持久化，每次状态变更后整体写入一份快照，快照内容包括：
版本号、阶段、活动会话、**静态实例身份/会话版本/在线状态/离线时间与保留期限**、
生效所有权、目标所有权、各成员撤销义务与确认进度、**已退场会话栅栏记录**、位点、
幂等记录。

- `FileStore`：JSON 单文件（快照格式版本 3），写入采用「临时文件 + fsync +
  原子 rename」，读回时要么看到上一整份快照、要么看到新整份快照，不会读到半成品；
- `MemoryStore`：内存实现（默认），用于测试或无需跨进程持久化的场景。

进程重启后用同一 `Store` 重建 `Coordinator` 即可恢复——**包括撤销阶段中途的
状态**与**保留期内暂时离线的静态实例**：恢复后旧成员仍可继续确认撤销、版本在
同一 generation 内收敛，离线实例也能在保留期内重连继续原分配。加载时会做跨字段
一致性校验（阶段、生效/目标所有权、撤销义务、存活成员、静态会话版本与离线字段、
所有权归属之间必须自洽），损坏的快照直接报错而不是带病运行。

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

静态成员的重连与清退：

```go
// 实例 worker-7 首次加入，保留期 5 分钟；返回会话版本 1 与会话 ID
j, _ := c.JoinStatic("orders", consumergroups.StaticJoinOptions{
    InstanceID: "worker-7", SessionID: "worker-7:pid:1001", Retention: 5 * time.Minute,
})
// 后续心跳 / 撤销确认 / 位点提交都带 (j.SessionID, j.SessionVersion=1)
c.HeartbeatV2("orders", consumergroups.HeartbeatRequest{
    MemberID: j.SessionID, Generation: j.Generation, SessionVersion: j.SessionVersion,
})

// 进程崩溃后，超过 SessionTimeout 会被标记为「暂时离线」，但 5 分钟内分区不动、
// 分配版本不变。新进程在保留期内重连：
r, _ := c.JoinStatic("orders", consumergroups.StaticJoinOptions{
    InstanceID: "worker-7", SessionID: "worker-7:pid:1002", Retention: 5 * time.Minute,
})
// r.Rejoined == true、r.SessionVersion == 2、r.Generation 与断线前相同，继续原分配。
// 旧进程 (pid:1001, version 1) 之后的心跳/确认/提交全部返回 ErrFencedSession。

// 真正退出三选一：超过保留期被扫描清退，或：
c.LeaveStatic("orders", "worker-7")  // 主动退出
c.RemoveInstance("orders", "worker-7") // 管理员移除（离线实例也可）
// 其原持有分区作为一整批只移交给唯一新所有者，立即纳入协作式再均衡。
```

## API 一览

```go
// 组管理
c.CreateGroup(consumergroups.CreateGroupOptions{Name: "orders", Partitions: 8, SessionTimeout: 10 * time.Second})
st, _ := c.Status("orders")     // 完整状态：生效/目标所有权、待撤销集合、确认进度、位点
names := c.Groups()             // 所有组名

// 成员生命周期（动态成员）
join, _ := c.Join("orders", "consumer-1")                 // 新版本 + 生效/目标所有权
hb, _ := c.Heartbeat("orders", "consumer-1", join.Generation) // 返回阶段与待撤销分区
ack, _ := c.AckRevocation("orders", consumergroups.RevocationAckRequest{
    MemberID: "consumer-1", Generation: join.Generation, Partitions: hb.Revoking,
})
c.Leave("orders", "consumer-1")
expired, _ := c.ExpireGroup("orders", time.Now())          // 或 c.ExpireAll(now) 周期扫描

// 静态成员：稳定实例标识 + 会话版本 + 保留期
sj, _ := c.JoinStatic("orders", consumergroups.StaticJoinOptions{
    InstanceID: "worker-7", SessionID: "worker-7:pid:1001", Retention: 5 * time.Minute,
})
c.HeartbeatV2("orders", consumergroups.HeartbeatRequest{ // 带会话版本的心跳
    MemberID: sj.SessionID, Generation: sj.Generation, SessionVersion: sj.SessionVersion,
})
c.LeaveStatic("orders", "worker-7")   // 主动退出（整批移交唯一后继）
c.RemoveInstance("orders", "worker-7") // 管理员移除

// 位点提交（只允许当前生效所有者；静态成员带 SessionID + SessionVersion）
res, err := c.CommitOffset("orders", consumergroups.CommitRequest{
    MemberID: sj.SessionID, SessionVersion: sj.SessionVersion, Generation: sj.Generation,
    Partition: 0, Offset: 1024, RequestID: "commit-0001",
})
```

## 错误判别

所有错误都可用 `errors.Is` 分类，关键错误同时提供带上下文的结构化类型，
可用 `errors.As` 提取：

| 哨兵错误 | 结构化类型 | 含义 |
| --- | --- | --- |
| `ErrIllegalGeneration` | `*GenerationMismatchError` | 携带的分配版本与当前版本不一致（含 `Want`/`Got`）；旧版本心跳/提交/撤销确认一律拒绝 |
| `ErrFencedSession` | `*FencedSessionError` | 操作来自静态实例的过期会话（更低会话版本，或断线后未以更高版本重连）；旧进程的心跳/撤销确认/位点提交一律拒绝（含实例与双方会话版本） |
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
| `coordinator.go` | 协调器：版本状态机、动态/静态加入与会话栅栏、心跳/离开/两级超时/撤销确认/提交/查询，锁内串行化 |
| `assign.go` | 确定性分区分配（保留期离线分区钉住、清退批次整批单后继）与 leader 选举 |
| `types.go` | 对外类型：`Assignment`、`GroupStatus`、`StaticJoinOptions/Result`、`HeartbeatRequest`、`RevocationAckRequest`、`CommitRequest` 等 |
| `errors.go` | 哨兵错误与结构化错误类型 |
| `store.go` | `Store` 接口、快照模型与恢复时的一致性校验 |
| `memory_store.go` / `file_store.go` | 内存 / 原子 JSON 文件持久化 |
