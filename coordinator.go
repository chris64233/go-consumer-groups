package consumergroups

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Config 是协调器的配置。
type Config struct {
	// SessionTimeout 是会话超时：成员超过该时长未心跳即被剔除。
	SessionTimeout time.Duration
	// Storage 为持久化存储；为 nil 时不持久化。
	Storage Storage
	// Now 为时钟，默认 time.Now；测试中可注入假时钟推进超时。
	Now func() time.Time
}

// Coordinator 是消费组协调器。所有变更操作在同一把互斥锁下串行执行，
// 因此成员离开、超时扫描与心跳无论怎么并发，都只会形成一条
// 清晰递增的分配版本（generation）序列；再均衡结果整体替换发布，
// 读者只能看到某个完整版本的分配，绝不会读到半套新分配。
type Coordinator struct {
	mu             sync.Mutex
	sessionTimeout time.Duration
	storage        Storage
	now            func() time.Time
	groups         map[string]*group
}

// NewCoordinator 创建协调器；若配置了 Storage 且存在快照，则恢复上次状态。
func NewCoordinator(cfg Config) (*Coordinator, error) {
	if cfg.SessionTimeout <= 0 {
		return nil, fmt.Errorf("%w: session timeout must be positive", ErrInvalidRequest)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	c := &Coordinator{
		sessionTimeout: cfg.SessionTimeout,
		storage:        cfg.Storage,
		now:            now,
		groups:         make(map[string]*group),
	}
	if cfg.Storage != nil {
		data, err := cfg.Storage.Load()
		if err != nil {
			return nil, err
		}
		if data != nil {
			groups, err := decodeSnapshot(data)
			if err != nil {
				return nil, err
			}
			c.groups = groups
		}
	}
	return c, nil
}

// CreateGroup 创建消费组，topics 声明组订阅的主题及固定分区数。
func (c *Coordinator) CreateGroup(groupID string, topics []TopicConfig) error {
	if groupID == "" {
		return fmt.Errorf("%w: empty group id", ErrInvalidRequest)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.groups[groupID]; ok {
		return fmt.Errorf("%w: %q", ErrGroupExists, groupID)
	}
	topicMap := make(map[string]int, len(topics))
	for _, t := range topics {
		if t.Name == "" || t.Partitions <= 0 {
			return fmt.Errorf("%w: bad topic config %+v", ErrInvalidRequest, t)
		}
		topicMap[t.Name] = t.Partitions
	}
	c.groups[groupID] = &group{
		id:         groupID,
		topics:     topicMap,
		generation: 0,
		members:    make(map[string]*member),
		assignment: Assignment{},
		offsets:    map[string]map[int]OffsetState{},
	}
	return c.persistLocked()
}

// JoinGroup 让成员加入组，返回当前分配版本与该成员分到的分区。
// 新成员加入会递增分配版本并触发整体再均衡；已在组内的成员
// 重复 Join 视为心跳续期，返回当前版本，不触发再均衡。
func (c *Coordinator) JoinGroup(groupID, memberID string) (int64, Assignment, error) {
	if memberID == "" {
		return 0, nil, fmt.Errorf("%w: empty member id", ErrInvalidRequest)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.getGroupLocked(groupID)
	if err != nil {
		return 0, nil, err
	}
	if _, ok := g.members[memberID]; ok {
		g.members[memberID].lastHeartbeat = c.now()
		return g.generation, assignmentOf(g.assignment, memberID), c.persistLocked()
	}
	g.members[memberID] = &member{id: memberID, lastHeartbeat: c.now()}
	c.rebalanceLocked(g)
	return g.generation, assignmentOf(g.assignment, memberID), c.persistLocked()
}

// Heartbeat 上报心跳。必须携带成员当前持有的分配版本；
// 携带旧版本的心跳说明该成员已被再均衡淘汰，返回 ErrStaleGeneration。
func (c *Coordinator) Heartbeat(groupID, memberID string, generation int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.getGroupLocked(groupID)
	if err != nil {
		return err
	}
	m, ok := g.members[memberID]
	if !ok {
		return fmt.Errorf("%w: %q in group %q", ErrMemberNotFound, memberID, groupID)
	}
	if generation != g.generation {
		return fmt.Errorf("%w: member %q holds %d, current is %d",
			ErrStaleGeneration, memberID, generation, g.generation)
	}
	m.lastHeartbeat = c.now()
	return c.persistLocked()
}

// LeaveGroup 让成员主动离开组，递增分配版本并整体再均衡。
func (c *Coordinator) LeaveGroup(groupID, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.getGroupLocked(groupID)
	if err != nil {
		return err
	}
	if _, ok := g.members[memberID]; !ok {
		return fmt.Errorf("%w: %q in group %q", ErrMemberNotFound, memberID, groupID)
	}
	delete(g.members, memberID)
	c.rebalanceLocked(g)
	return c.persistLocked()
}

// ScanTimeouts 扫描所有组，剔除会话超时的成员并触发再均衡，
// 返回被剔除的成员（groupID -> memberIDs）。与心跳、离开并发时
// 由互斥锁串行化，只会产生一条递增的版本序列。
func (c *Coordinator) ScanTimeouts() (map[string][]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	deadline := c.now().Add(-c.sessionTimeout)
	expired := make(map[string][]string)
	for id, g := range c.groups {
		var removed []string
		for mid, m := range g.members {
			if m.lastHeartbeat.Before(deadline) {
				delete(g.members, mid)
				removed = append(removed, mid)
			}
		}
		if len(removed) > 0 {
			sort.Strings(removed)
			c.rebalanceLocked(g)
			expired[id] = removed
		}
	}
	if len(expired) > 0 {
		if err := c.persistLocked(); err != nil {
			return nil, err
		}
	}
	return expired, nil
}

// CommitOffset 提交位点。只有当前版本下该分区的所有者才能提交；
// 携带旧版本的提交返回 ErrStaleGeneration，非所有者返回
// ErrNotPartitionOwner。位点默认不得后退（ErrOffsetRegression）。
//
// requestID 提供幂等性：同一请求号重投相同位点视为成功（幂等重放），
// 同一请求号改交不同位点返回 ErrCommitConflict。所有提交在锁内串行，
// 并发提交中较大的合法位点不会因竞态丢失。
func (c *Coordinator) CommitOffset(groupID, memberID string, generation int64, topic string, partition int, offset int64, requestID string) error {
	if requestID == "" {
		return fmt.Errorf("%w: empty request id", ErrInvalidRequest)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.getGroupLocked(groupID)
	if err != nil {
		return err
	}
	if _, ok := g.members[memberID]; !ok {
		return fmt.Errorf("%w: %q in group %q", ErrMemberNotFound, memberID, groupID)
	}
	if generation != g.generation {
		return fmt.Errorf("%w: member %q holds %d, current is %d",
			ErrStaleGeneration, memberID, generation, g.generation)
	}
	parts, ok := g.topics[topic]
	if !ok {
		return fmt.Errorf("%w: %q in group %q", ErrUnknownTopic, topic, groupID)
	}
	if partition < 0 || partition >= parts {
		return fmt.Errorf("%w: %s[%d]", ErrUnknownPartition, topic, partition)
	}
	if owner := ownerOf(g.assignment, topic, partition); owner != memberID {
		return fmt.Errorf("%w: %s[%d] owned by %q, not %q",
			ErrNotPartitionOwner, topic, partition, owner, memberID)
	}

	topicOffsets, ok := g.offsets[topic]
	if !ok {
		topicOffsets = make(map[int]OffsetState)
		g.offsets[topic] = topicOffsets
	}
	if cur, ok := topicOffsets[partition]; ok {
		if cur.RequestID == requestID {
			if cur.Offset == offset {
				return nil // 幂等重放
			}
			return fmt.Errorf("%w: request %q committed %d, cannot recommit %d",
				ErrCommitConflict, requestID, cur.Offset, offset)
		}
		if offset < cur.Offset {
			return fmt.Errorf("%w: %s[%d] committed %d, cannot move back to %d",
				ErrOffsetRegression, topic, partition, cur.Offset, offset)
		}
	}
	topicOffsets[partition] = OffsetState{Offset: offset, RequestID: requestID}
	return c.persistLocked()
}

// State 返回组状态的整体快照（深拷贝），调用方可安全读取。
func (c *Coordinator) State(groupID string) (GroupState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.getGroupLocked(groupID)
	if err != nil {
		return GroupState{}, err
	}
	state := GroupState{
		ID:         g.id,
		Generation: g.generation,
		Topics:     make(map[string]int, len(g.topics)),
		Members:    make([]MemberState, 0, len(g.members)),
		Assignment: copyAssignment(g.assignment),
		Offsets:    make(map[string]map[int]OffsetState, len(g.offsets)),
	}
	for name, parts := range g.topics {
		state.Topics[name] = parts
	}
	for _, m := range g.members {
		state.Members = append(state.Members, MemberState{ID: m.id, LastHeartbeat: m.lastHeartbeat})
	}
	sort.Slice(state.Members, func(i, j int) bool { return state.Members[i].ID < state.Members[j].ID })
	for topic, offsets := range g.offsets {
		cp := make(map[int]OffsetState, len(offsets))
		for p, o := range offsets {
			cp[p] = o
		}
		state.Offsets[topic] = cp
	}
	return state, nil
}

// getGroupLocked 查找组，调用方必须已持有锁。
func (c *Coordinator) getGroupLocked(groupID string) (*group, error) {
	g, ok := c.groups[groupID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupID)
	}
	return g, nil
}

// rebalanceLocked 递增分配版本并整体替换分配结果，调用方必须已持有锁。
func (c *Coordinator) rebalanceLocked(g *group) {
	g.generation++
	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	g.assignment = computeAssignment(g.topics, memberIDs)
}

// persistLocked 持久化当前状态，调用方必须已持有锁。
func (c *Coordinator) persistLocked() error {
	if c.storage == nil {
		return nil
	}
	data, err := encodeSnapshot(c.groups)
	if err != nil {
		return err
	}
	return c.storage.Save(data)
}

// assignmentOf 提取某个成员分到的分区（topic -> partitions）。
func assignmentOf(a Assignment, memberID string) Assignment {
	out := Assignment{}
	for topic, parts := range a {
		for p, owner := range parts {
			if owner == memberID {
				if out[topic] == nil {
					out[topic] = map[int]string{}
				}
				out[topic][p] = memberID
			}
		}
	}
	return out
}

func copyAssignment(a Assignment) Assignment {
	out := make(Assignment, len(a))
	for topic, parts := range a {
		cp := make(map[int]string, len(parts))
		for p, owner := range parts {
			cp[p] = owner
		}
		out[topic] = cp
	}
	return out
}
