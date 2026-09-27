package consumergroups

import (
	"sort"
	"time"
)

// Snapshot 是协调器完整状态的持久化表示。一次 Save 对应一条分配版本
// 序列中的某个确定快照；Store 实现必须保证写入整体可见——读回时要么
// 看到上一整份快照，要么看到新整份快照，不会读到半份状态。
type Snapshot struct {
	Groups []GroupSnapshot
}

// GroupSnapshot 是单个组的持久化状态。
type GroupSnapshot struct {
	Name           string
	Partitions     int
	SessionTimeout time.Duration
	Generation     int64
	Leader         string
	LastRebalance  time.Time
	Members        []MemberSnapshot
	// Assignment 当前有效所有权。
	Assignment Assignment
	// Target 本代目标所有权。
	Target Assignment
	// PendingRevocations 待撤销集合。
	PendingRevocations []PartitionRevocation
	// Acks 各成员本代的撤销确认/强制回收进度。
	Acks    []MemberAckSnapshot
	Offsets []Offset
}

// MemberAckSnapshot 是单个成员撤销进度的持久化状态。
type MemberAckSnapshot struct {
	MemberID       string
	Acked          []int
	ForceReclaimed []int
}

// MemberSnapshot 是单个成员的持久化状态。
type MemberSnapshot struct {
	ID              string
	JoinedAt        time.Time
	LastHeartbeatAt time.Time
	Requests        []RequestSnapshot
}

// RequestSnapshot 是一条幂等请求记录的持久化状态。
type RequestSnapshot struct {
	RequestID   string
	Partition   int
	Offset      int64
	Metadata    string
	CommittedAt time.Time
}

// Store 抽象协调状态的持久化。
type Store interface {
	// Save 原子地写入整份快照。
	Save(Snapshot) error
	// Load 读回最近一次 Save 的快照；从未写入过时返回 (nil, nil)。
	Load() (*Snapshot, error)
}

// snapshotLocked 生成当前完整状态的快照（深拷贝，落盘期间不受后续变更影响）。
func (c *Coordinator) snapshotLocked() Snapshot {
	snap := Snapshot{Groups: make([]GroupSnapshot, 0, len(c.groups))}
	names := make([]string, 0, len(c.groups))
	for name := range c.groups {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		g := c.groups[name]

		members := make([]MemberSnapshot, 0, len(g.members))
		for id, m := range g.members {
			reqs := make([]RequestSnapshot, 0, len(m.requests))
			for rid, rec := range m.requests {
				reqs = append(reqs, RequestSnapshot{
					RequestID: rid, Partition: rec.partition, Offset: rec.offset,
					Metadata: rec.metadata, CommittedAt: rec.committedAt,
				})
			}
			sortRequests(reqs)
			members = append(members, MemberSnapshot{
				ID: id, JoinedAt: m.joinedAt, LastHeartbeatAt: m.lastHeartbeatAt, Requests: reqs,
			})
		}
		sortMembers(members)

		offsets := make([]Offset, 0, len(g.offsets))
		for _, o := range g.offsets {
			offsets = append(offsets, *o)
		}
		sortOffsets(offsets)

		pending := make([]PartitionRevocation, 0, len(g.pending))
		for _, rev := range g.pending {
			pending = append(pending, PartitionRevocation{
				Partition: rev.partition, From: rev.from, To: rev.to,
			})
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].Partition < pending[j].Partition })

		ackIDs := make(map[string]bool, len(g.acked)+len(g.reclaimed))
		for id := range g.acked {
			ackIDs[id] = true
		}
		for id := range g.reclaimed {
			ackIDs[id] = true
		}
		acks := make([]MemberAckSnapshot, 0, len(ackIDs))
		for id := range ackIDs {
			acks = append(acks, MemberAckSnapshot{
				MemberID:       id,
				Acked:          append([]int(nil), g.acked[id]...),
				ForceReclaimed: append([]int(nil), g.reclaimed[id]...),
			})
		}
		sort.Slice(acks, func(i, j int) bool { return acks[i].MemberID < acks[j].MemberID })

		snap.Groups = append(snap.Groups, GroupSnapshot{
			Name:               g.name,
			Partitions:         g.partitions,
			SessionTimeout:     g.sessionTimeout,
			Generation:         g.generation,
			Leader:             g.leader,
			LastRebalance:      g.lastRebalance,
			Members:            members,
			Assignment:         g.assignment.clone(),
			Target:             g.target.clone(),
			PendingRevocations: pending,
			Acks:               acks,
			Offsets:            offsets,
		})
	}
	return snap
}

// restore 从快照重建协调器内存状态。
func (c *Coordinator) restore(snap *Snapshot) error {
	for _, gs := range snap.Groups {
		if gs.Partitions <= 0 {
			return errCorrupt("group %q has non-positive partition count %d", gs.Name, gs.Partitions)
		}
		g := &group{
			name:           gs.Name,
			partitions:     gs.Partitions,
			sessionTimeout: gs.SessionTimeout,
			generation:     gs.Generation,
			leader:         gs.Leader,
			lastRebalance:  gs.LastRebalance,
			members:        make(map[string]*member, len(gs.Members)),
			offsets:        make(map[int]*Offset, len(gs.Offsets)),
			assignment:     gs.Assignment.clone(),
			target:         gs.Target.clone(),
			pending:        make(map[int]revocation, len(gs.PendingRevocations)),
			acked:          make(map[string][]int, len(gs.Acks)),
			reclaimed:      make(map[string][]int, len(gs.Acks)),
		}
		if g.assignment.Owners == nil {
			g.assignment.Owners = make([]string, gs.Partitions)
		}
		if len(g.assignment.Owners) != gs.Partitions {
			return errCorrupt("group %q assignment length %d != partitions %d",
				gs.Name, len(g.assignment.Owners), gs.Partitions)
		}
		if g.assignment.Generation != gs.Generation {
			return errCorrupt("group %q assignment generation %d != group generation %d",
				gs.Name, g.assignment.Generation, gs.Generation)
		}
		if len(g.target.Owners) == 0 && len(gs.PendingRevocations) == 0 {
			// 旧格式快照（无协作式字段）：视为再均衡已完成。
			g.target = g.assignment.clone()
		}
		if g.target.Owners == nil {
			g.target.Owners = make([]string, gs.Partitions)
		}
		if len(g.target.Owners) != gs.Partitions {
			return errCorrupt("group %q target assignment length %d != partitions %d",
				gs.Name, len(g.target.Owners), gs.Partitions)
		}
		if g.target.Generation != gs.Generation {
			return errCorrupt("group %q target generation %d != group generation %d",
				gs.Name, g.target.Generation, gs.Generation)
		}
		for _, pr := range gs.PendingRevocations {
			if pr.Partition < 0 || pr.Partition >= gs.Partitions {
				return errCorrupt("group %q pending revocation partition %d out of range",
					gs.Name, pr.Partition)
			}
			if g.assignment.OwnerOf(pr.Partition) != pr.From {
				return errCorrupt("group %q pending revocation partition %d from=%q but effective owner=%q",
					gs.Name, pr.Partition, pr.From, g.assignment.OwnerOf(pr.Partition))
			}
			if g.target.OwnerOf(pr.Partition) != pr.To {
				return errCorrupt("group %q pending revocation partition %d to=%q but target owner=%q",
					gs.Name, pr.Partition, pr.To, g.target.OwnerOf(pr.Partition))
			}
			g.pending[pr.Partition] = revocation{partition: pr.Partition, from: pr.From, to: pr.To}
		}
		for _, as := range gs.Acks {
			if len(as.Acked) > 0 {
				g.acked[as.MemberID] = append([]int(nil), as.Acked...)
			}
			if len(as.ForceReclaimed) > 0 {
				g.reclaimed[as.MemberID] = append([]int(nil), as.ForceReclaimed...)
			}
		}

		for _, ms := range gs.Members {
			m := &member{
				id:              ms.ID,
				joinedAt:        ms.JoinedAt,
				lastHeartbeatAt: ms.LastHeartbeatAt,
				requests:        make(map[string]requestRecord, len(ms.Requests)),
			}
			for _, rs := range ms.Requests {
				m.requests[rs.RequestID] = requestRecord{
					partition: rs.Partition, offset: rs.Offset,
					metadata: rs.Metadata, committedAt: rs.CommittedAt,
				}
			}
			g.members[ms.ID] = m
		}
		for i := range gs.Offsets {
			o := gs.Offsets[i]
			g.offsets[o.Partition] = &o
		}
		c.groups[gs.Name] = g
	}
	return nil
}

// 以下排序让序列化输出保持稳定顺序，便于测试断言与人工排查
// （避免 map 迭代顺序噪声）。
func sortStrings(s []string) { sort.Strings(s) }

func sortMembers(m []MemberSnapshot) {
	sort.Slice(m, func(i, j int) bool { return m[i].ID < m[j].ID })
}

func sortOffsets(o []Offset) {
	sort.Slice(o, func(i, j int) bool { return o[i].Partition < o[j].Partition })
}

func sortRequests(r []RequestSnapshot) {
	sort.Slice(r, func(i, j int) bool { return r[i].RequestID < r[j].RequestID })
}
