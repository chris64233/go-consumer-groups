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
	// StaticRetention 静态成员离线保留期。
	StaticRetention time.Duration
	Generation      int64
	// Phase 版本状态机当前阶段（stable / revoking）。
	Phase         RebalancePhase
	Leader        string
	LastRebalance time.Time
	Members       []MemberSnapshot
	// Assignment 当前生效所有权。
	Assignment Assignment
	// TargetAssignment 本版本目标所有权；stable 时与 Assignment 一致。
	TargetAssignment Assignment
	// Revocations 撤销阶段各成员的应撤销/已确认分区；stable 时为空。
	Revocations []RevocationSnapshot
	Offsets     []Offset
}

// RevocationSnapshot 是单个成员在当前版本下的撤销义务与确认进度。
type RevocationSnapshot struct {
	MemberID string
	// Required 应撤销分区（升序）。
	Required []int
	// Acked 已确认撤销分区（升序，Required 的子集）。
	Acked []int
}

// MemberSnapshot 是单个成员的持久化状态。
type MemberSnapshot struct {
	ID              string
	JoinedAt        time.Time
	LastHeartbeatAt time.Time
	Requests        []RequestSnapshot
	// Static 是否为静态成员（稳定实例身份 + 会话版本 + 离线保留期）。
	Static bool
	// Online 是否有活跃会话；静态成员离线保留期内为 false 但身份仍在。
	Online bool
	// SessionVersion 当前会话版本；静态成员严格递增，动态成员为 0。
	SessionVersion int64
	// OfflineAt / RetainUntil 静态成员离线保留期起止；在线时为零值。
	OfflineAt   time.Time
	RetainUntil time.Time
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
				ID:              id,
				JoinedAt:        m.joinedAt,
				LastHeartbeatAt: m.lastHeartbeatAt,
				Requests:        reqs,
				Static:          m.static,
				Online:          m.online,
				SessionVersion:  m.sessionVersion,
				OfflineAt:       m.offlineAt,
				RetainUntil:     m.retainUntil,
			})
		}
		sortMembers(members)

		offsets := make([]Offset, 0, len(g.offsets))
		for _, o := range g.offsets {
			offsets = append(offsets, *o)
		}
		sortOffsets(offsets)

		revocations := make([]RevocationSnapshot, 0, len(g.revokeRequired))
		for id, set := range g.revokeRequired {
			revocations = append(revocations, RevocationSnapshot{
				MemberID: id,
				Required: sortedSet(set),
				Acked:    sortedSet(g.revokeAcked[id]),
			})
		}
		sortRevocations(revocations)

		snap.Groups = append(snap.Groups, GroupSnapshot{
			Name:             g.name,
			Partitions:       g.partitions,
			SessionTimeout:   g.sessionTimeout,
			StaticRetention:  g.staticRetention,
			Generation:       g.generation,
			Phase:            g.phase,
			Leader:           g.leader,
			LastRebalance:    g.lastRebalance,
			Members:          members,
			Assignment:       g.assignment.clone(),
			TargetAssignment: g.target.clone(),
			Revocations:      revocations,
			Offsets:          offsets,
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
			name:            gs.Name,
			partitions:      gs.Partitions,
			sessionTimeout:  gs.SessionTimeout,
			staticRetention: gs.StaticRetention,
			generation:      gs.Generation,
			phase:           gs.Phase,
			leader:          gs.Leader,
			lastRebalance:   gs.LastRebalance,
			members:         make(map[string]*member, len(gs.Members)),
			offsets:         make(map[int]*Offset, len(gs.Offsets)),
			assignment:      gs.Assignment.clone(),
			target:          gs.TargetAssignment.clone(),
		}
		if g.phase != PhaseStable && g.phase != PhaseRevoking {
			return errCorrupt("group %q has unknown phase %q", gs.Name, gs.Phase)
		}
		if g.staticRetention <= 0 {
			g.staticRetention = DefaultStaticRetention
		}
		if g.assignment.Owners == nil {
			g.assignment.Owners = make([]string, gs.Partitions)
		}
		if g.target.Owners == nil {
			g.target.Owners = make([]string, gs.Partitions)
		}
		if len(g.assignment.Owners) != gs.Partitions {
			return errCorrupt("group %q assignment length %d != partitions %d",
				gs.Name, len(g.assignment.Owners), gs.Partitions)
		}
		if len(g.target.Owners) != gs.Partitions {
			return errCorrupt("group %q target assignment length %d != partitions %d",
				gs.Name, len(g.target.Owners), gs.Partitions)
		}
		if g.assignment.Generation != gs.Generation {
			return errCorrupt("group %q assignment generation %d != group generation %d",
				gs.Name, g.assignment.Generation, gs.Generation)
		}
		if g.target.Generation != gs.Generation {
			return errCorrupt("group %q target assignment generation %d != group generation %d",
				gs.Name, g.target.Generation, gs.Generation)
		}

		g.revokeRequired = make(map[string]map[int]struct{})
		g.revokeAcked = make(map[string]map[int]struct{})
		for _, rv := range gs.Revocations {
			req := make(map[int]struct{}, len(rv.Required))
			for _, p := range rv.Required {
				if p < 0 || p >= gs.Partitions {
					return errCorrupt("group %q revocation required partition %d out of range", gs.Name, p)
				}
				if _, dup := req[p]; dup {
					return errCorrupt("group %q revocation required has duplicate partition %d", gs.Name, p)
				}
				req[p] = struct{}{}
			}
			acked := make(map[int]struct{}, len(rv.Acked))
			for _, p := range rv.Acked {
				if _, ok := req[p]; !ok {
					return errCorrupt("group %q member %q acked partition %d not in required set",
						gs.Name, rv.MemberID, p)
				}
				if _, dup := acked[p]; dup {
					return errCorrupt("group %q revocation acked has duplicate partition %d", gs.Name, p)
				}
				acked[p] = struct{}{}
			}
			g.revokeRequired[rv.MemberID] = req
			// 仅当确有已确认分区时才记入 acked，保持「revokeAcked 的 key 数 ==
			// 已完成确认的成员数」这一不变量（与运行时 rebalanceLocked 一致）。
			if len(acked) > 0 {
				g.revokeAcked[rv.MemberID] = acked
			}
		}

		for _, ms := range gs.Members {
			// 动态成员没有离线保留态：持久化字段即使缺失（零值）也按在线处理。
			if !ms.Static {
				ms.Online = true
			}
			if ms.Static && ms.SessionVersion <= 0 {
				return errCorrupt("group %q static member %q has non-positive session version %d",
					gs.Name, ms.ID, ms.SessionVersion)
			}
			if !ms.Static {
				if ms.SessionVersion != 0 || !ms.OfflineAt.IsZero() || !ms.RetainUntil.IsZero() {
					return errCorrupt("group %q dynamic member %q carries static session fields",
						gs.Name, ms.ID)
				}
			} else if !ms.Online {
				if ms.RetainUntil.IsZero() || ms.OfflineAt.IsZero() {
					return errCorrupt("group %q offline static member %q missing offline/retain timestamps",
						gs.Name, ms.ID)
				}
			} else if !ms.OfflineAt.IsZero() || !ms.RetainUntil.IsZero() {
				return errCorrupt("group %q online static member %q carries offline timestamps",
					gs.Name, ms.ID)
			}
			m := &member{
				id:              ms.ID,
				static:          ms.Static,
				online:          ms.Online,
				sessionVersion:  ms.SessionVersion,
				joinedAt:        ms.JoinedAt,
				lastHeartbeatAt: ms.LastHeartbeatAt,
				offlineAt:       ms.OfflineAt,
				retainUntil:     ms.RetainUntil,
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

		// leader 若非空必须是在线成员（离线保留期内的静态实例不能担任 leader）。
		if g.leader != "" {
			lm, ok := g.members[g.leader]
			if !ok {
				return errCorrupt("group %q leader %q is not a member", gs.Name, g.leader)
			}
			if lm.static && !lm.online {
				return errCorrupt("group %q leader %q is offline", gs.Name, g.leader)
			}
		}
		// 注：离线静态成员可能在撤销阶段中途断线，因而仍带撤销义务（在途分区
		// 生效所有权指向它、目标指向别人），这是合法状态——重连后由新会话确认，
		// 或保留期届满后被整批强制回收。通用在途分区校验（下方）已覆盖该情形。

		// 跨字段一致性：阶段与撤销义务集合、生效/目标所有权必须自洽。
		if g.phase == PhaseStable {
			if len(g.revokeRequired) > 0 {
				return errCorrupt("group %q is stable but has %d pending revocations",
					gs.Name, len(g.revokeRequired))
			}
			if !equalStringSlices(g.assignment.Owners, g.target.Owners) {
				return errCorrupt("group %q is stable but effective and target owners differ", gs.Name)
			}
		} else {
			if len(g.revokeRequired) == 0 {
				return errCorrupt("group %q is revoking but has no required revocations", gs.Name)
			}
			inFlight := make(map[int]string)
			for p := 0; p < gs.Partitions; p++ {
				eff, tgt := g.assignment.Owners[p], g.target.Owners[p]
				if eff == tgt {
					continue // 已移交 / 未受影响 / 无主
				}
				// 仍由旧主持有的在途分区：旧主必须存活且该分区在其撤销义务中，
				// 且尚未被确认。
				if _, alive := g.members[eff]; !alive {
					return errCorrupt("group %q partition %d effective owner %q not a member",
						gs.Name, p, eff)
				}
				if _, ok := g.revokeRequired[eff][p]; !ok {
					return errCorrupt("group %q partition %d in flight but missing from %q revocation set",
						gs.Name, p, eff)
				}
				if _, isAcked := g.revokeAcked[eff][p]; isAcked {
					return errCorrupt("group %q partition %d in flight but already acked by %q",
						gs.Name, p, eff)
				}
				inFlight[p] = eff
			}
			// 反向校验：撤销义务中的每个分区要么仍在途由该成员持有（未确认），
			// 要么已确认并完成转移（生效==目标，且目标不再是该成员）。
			for memberID, set := range g.revokeRequired {
				if _, alive := g.members[memberID]; !alive {
					return errCorrupt("group %q revocation obligation belongs to non-member %q",
						gs.Name, memberID)
				}
				ackedSet := g.revokeAcked[memberID]
				for p := range set {
					if _, isAcked := ackedSet[p]; isAcked {
						eff, tgt := g.assignment.Owners[p], g.target.Owners[p]
						if eff != tgt || tgt == memberID {
							return errCorrupt("group %q partition %d acked by %q but not transferred (eff=%q tgt=%q)",
								gs.Name, p, memberID, eff, tgt)
						}
						continue
					}
					if inFlight[p] != memberID {
						return errCorrupt("group %q partition %d not in flight from %q but in its revocation set",
							gs.Name, p, memberID)
					}
				}
			}
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

func sortRevocations(r []RevocationSnapshot) {
	sort.Slice(r, func(i, j int) bool { return r[i].MemberID < r[j].MemberID })
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
