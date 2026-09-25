# go-consumer-groups

用于承载分区消费成员与位点管理相关的 Go 服务代码。

开发环境：Go 1.23.0。

运行测试：

    go test ./...

## 消费组协调器

`consumergroups` 包实现了一个以**递增分配版本（generation）**隔离旧成员操作的
消费组协调器，核心语义对标 Kafka 消费组的 generation / rebalance 模型。

### 核心概念

- **固定分区主题**：创建组时声明订阅主题及分区数，分区数固定不变。
- **分配版本（generation）**：成员加入、主动离开、会话超时被剔除都会使版本
  递增 1 并触发整体再均衡。版本只增不减，形成唯一一条清晰的版本序列。
- **确定性分配**：主题按名称、成员按 ID 排序后，分区按全局序号轮询分配。
  相同成员集合必然得到相同分配；同一版本内一个分区至多归属一个成员。
- **整体发布**：再均衡结果在锁内整体替换，读者（`State`）只能看到某个完整
  版本的分配，绝不会读到半套新分配。

### API

| 方法 | 说明 |
| --- | --- |
| `CreateGroup(groupID, topics)` | 创建组并声明固定分区主题 |
| `JoinGroup(groupID, memberID)` | 加入组，返回当前版本与该成员的分区；新成员触发再均衡 |
| `Heartbeat(groupID, memberID, generation)` | 心跳续期，携带旧版本返回 `ErrStaleGeneration` |
| `LeaveGroup(groupID, memberID)` | 主动离开，触发再均衡 |
| `ScanTimeouts()` | 剔除会话超时成员并再均衡，返回被剔除者 |
| `CommitOffset(...)` | 提交位点（见下文约束） |
| `State(groupID)` | 查询组状态整体快照（深拷贝） |

### 并发与版本隔离

所有变更操作在同一把互斥锁下串行执行，因此成员离开、超时扫描与心跳无论
怎么并发，都只会产生一条递增的版本序列。被新版本淘汰的迟到操作（旧版本的
心跳、位点提交）会被拒绝，既不能复活已移除的成员，也不能覆盖新分配。

### 位点提交约束

- 只有**当前版本下该分区的所有者**才能提交（否则 `ErrNotPartitionOwner`）；
  携带旧版本提交返回 `ErrStaleGeneration`。
- 位点默认**不得后退**（`ErrOffsetRegression`）。
- **请求号幂等**：同一请求号重投相同位点视为成功（幂等重放）；同一请求号
  改交不同位点返回 `ErrCommitConflict`。
- 提交在锁内串行，并发提交不会丢失较大的合法位点。

### 错误判别

所有领域错误都是 `errors.go` 中导出的哨兵错误，用 `errors.Is` 判别：
`ErrGroupExists`、`ErrGroupNotFound`、`ErrMemberNotFound`、
`ErrStaleGeneration`、`ErrNotPartitionOwner`、`ErrOffsetRegression`、
`ErrCommitConflict`、`ErrUnknownTopic`、`ErrUnknownPartition` 等。

### 持久化

配置 `Storage`（内置 `FileStorage`，临时文件 + rename 原子写入）后，每次
状态变更都会保存整体 JSON 快照；重启后通过 `NewCoordinator` 自动恢复版本、
成员、分配与位点，幂等语义跨重启保持有效。

### 测试

`coordinator_test.go` 覆盖：版本递增与确定性分配、旧版本心跳/提交拒绝、
迟到操作不能复活成员、会话超时再均衡、位点所有权/后退/幂等/冲突、并发提交
保留最大位点、并发成员变更下版本序列与分配完整性、再均衡整体发布、持久化
往返。并发测试需在 `-race` 下运行：

    go test -race ./...
