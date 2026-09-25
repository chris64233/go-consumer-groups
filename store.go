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
	Assignment     Assignment
	Offsets        []Offset
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

		snap.Groups = append(snap.Groups, GroupSnapshot{
			Name:           g.name,
			Partitions:     g.partitions,
			SessionTimeout: g.sessionTimeout,
			Generation:     g.generation,
			Leader:         g.leader,
			LastRebalance:  g.lastRebalance,
			Members:        members,
			Assignment:     g.assignment.clone(),
			Offsets:        offsets,
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
