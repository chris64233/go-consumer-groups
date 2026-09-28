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
	// Phase 版本状态机当前阶段（stable / revoking）。
	Phase         RebalancePhase
	Leader        string
	LastRebalance time.Time
	Members       []MemberSnapshot
	// StaticInstances 全部静态实例，含保留期内暂时离线（Online=false）者。
	StaticInstances []StaticInstanceSnapshot
	// DeadSessions 已退场（被接管或断线）的静态会话，用于拒绝旧进程迟到操作。
	DeadSessions []DeadSessionSnapshot
	// Assignment 当前生效所有权。
	Assignment Assignment
	// TargetAssignment 本版本目标所有权；stable 时与 Assignment 一致。
	TargetAssignment Assignment
	// Revocations 撤销阶段各成员的应撤销/已确认分区；stable 时为空。
	Revocations []RevocationSnapshot
	Offsets     []Offset
}

// StaticInstanceSnapshot 是静态成员持久身份的持久化状态（跨会话/重连存活）。
type StaticInstanceSnapshot struct {
	ID              string
	JoinedAt        time.Time
	Retention       time.Duration
	SessionVersion  int64
	Online          bool
	SessionID       string
	LastHeartbeatAt time.Time
	OfflineAt       time.Time
	RetainUntil     time.Time
	Requests        []RequestSnapshot
}

// DeadSessionSnapshot 是一条已退场静态会话的栅栏记录。
type DeadSessionSnapshot struct {
	SessionID string
	Instance  string
	Version   int64
}

// RevocationSnapshot 是单个成员在当前版本下的撤销义务与确认进度。
type RevocationSnapshot struct {
	MemberID string
	// Required 应撤销分区（升序）。
	Required []int
	// Acked 已确认撤销分区（升序，Required 的子集）。
	Acked []int
}

// MemberSnapshot 是单个活动会话的持久化状态。
type MemberSnapshot struct {
	ID string
	// Static / Instance / SessionVersion 仅静态会话使用。
	Static         bool
	Instance       string
	SessionVersion int64
	JoinedAt       time.Time
	// LastHeartbeatAt 动态成员为其心跳时间；静态会话为当前会话心跳时间。
	LastHeartbeatAt time.Time
	// Requests 仅动态成员持久化；静态成员的幂等记录归属于 StaticInstanceSnapshot。
	Requests []RequestSnapshot
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
			ms := MemberSnapshot{
				ID:              id,
				Static:          m.static,
				Instance:        m.instance,
				SessionVersion:  m.sessionVersion,
				JoinedAt:        m.joinedAt,
				LastHeartbeatAt: m.lastHeartbeatAt,
			}
			// 静态会话的幂等记录归属实例，避免重复落盘。
			if !m.static {
				reqs := make([]RequestSnapshot, 0, len(m.requests))
				for rid, rec := range m.requests {
					reqs = append(reqs, RequestSnapshot{
						RequestID: rid, Partition: rec.partition, Offset: rec.offset,
						Metadata: rec.metadata, CommittedAt: rec.committedAt,
					})
				}
				sortRequests(reqs)
				ms.Requests = reqs
			}
			members = append(members, ms)
		}
		sortMembers(members)

		instances := make([]StaticInstanceSnapshot, 0, len(g.instances))
		for id, inst := range g.instances {
			reqs := make([]RequestSnapshot, 0, len(inst.requests))
			for rid, rec := range inst.requests {
				reqs = append(reqs, RequestSnapshot{
					RequestID: rid, Partition: rec.partition, Offset: rec.offset,
					Metadata: rec.metadata, CommittedAt: rec.committedAt,
				})
			}
			sortRequests(reqs)
			instances = append(instances, StaticInstanceSnapshot{
				ID:              id,
				JoinedAt:        inst.joinedAt,
				Retention:       inst.retention,
				SessionVersion:  inst.version,
				Online:          inst.online,
				SessionID:       inst.sessionID,
				LastHeartbeatAt: inst.lastHeartbeatAt,
				OfflineAt:       inst.offlineAt,
				RetainUntil:     inst.retainUntil,
				Requests:        reqs,
			})
		}
		sortInstances(instances)

		dead := make([]DeadSessionSnapshot, 0, len(g.deadSessions))
		for sid, d := range g.deadSessions {
			dead = append(dead, DeadSessionSnapshot{
				SessionID: sid, Instance: d.instance, Version: d.version,
			})
		}
		sortDeadSessions(dead)

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
			Generation:       g.generation,
			Phase:            g.phase,
			Leader:           g.leader,
			LastRebalance:    g.lastRebalance,
			Members:          members,
			StaticInstances:  instances,
			DeadSessions:     dead,
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
			name:           gs.Name,
			partitions:     gs.Partitions,
			sessionTimeout: gs.SessionTimeout,
			generation:     gs.Generation,
			phase:          gs.Phase,
			leader:         gs.Leader,
			lastRebalance:  gs.LastRebalance,
			members:        make(map[string]*member, len(gs.Members)),
			instances:      make(map[string]*staticInstance, len(gs.StaticInstances)),
			deadSessions:   make(map[string]deadSession, len(gs.DeadSessions)),
			offsets:        make(map[int]*Offset, len(gs.Offsets)),
			assignment:     gs.Assignment.clone(),
			target:         gs.TargetAssignment.clone(),
		}
		if g.phase != PhaseStable && g.phase != PhaseRevoking {
			return errCorrupt("group %q has unknown phase %q", gs.Name, gs.Phase)
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

		// 先恢复静态实例身份（含保留期内离线者）。
		for _, is := range gs.StaticInstances {
			if is.ID == "" {
				return errCorrupt("group %q has static instance with empty id", gs.Name)
			}
			if _, dup := g.instances[is.ID]; dup {
				return errCorrupt("group %q static instance %q duplicated", gs.Name, is.ID)
			}
			if is.SessionVersion <= 0 {
				return errCorrupt("group %q static instance %q has non-positive session version %d",
					gs.Name, is.ID, is.SessionVersion)
			}
			if is.Retention <= 0 {
				return errCorrupt("group %q static instance %q has non-positive retention %s",
					gs.Name, is.ID, is.Retention)
			}
			if is.SessionID == "" {
				return errCorrupt("group %q static instance %q has empty session id", gs.Name, is.ID)
			}
			if !is.Online {
				if is.RetainUntil.IsZero() {
					return errCorrupt("group %q offline static instance %q has zero retain_until", gs.Name, is.ID)
				}
				if is.OfflineAt.IsZero() {
					return errCorrupt("group %q offline static instance %q has zero offline_at", gs.Name, is.ID)
				}
			}
			inst := &staticInstance{
				id:              is.ID,
				joinedAt:        is.JoinedAt,
				retention:       is.Retention,
				version:         is.SessionVersion,
				online:          is.Online,
				sessionID:       is.SessionID,
				lastHeartbeatAt: is.LastHeartbeatAt,
				offlineAt:       is.OfflineAt,
				retainUntil:     is.RetainUntil,
				requests:        make(map[string]requestRecord, len(is.Requests)),
			}
			for _, rs := range is.Requests {
				inst.requests[rs.RequestID] = requestRecord{
					partition: rs.Partition, offset: rs.Offset,
					metadata: rs.Metadata, committedAt: rs.CommittedAt,
				}
			}
			g.instances[is.ID] = inst
		}

		// 恢复活动会话（动态成员 + 在线静态实例的当前会话）。
		for _, ms := range gs.Members {
			if ms.ID == "" {
				return errCorrupt("group %q has member with empty id", gs.Name)
			}
			if _, dup := g.members[ms.ID]; dup {
				return errCorrupt("group %q member %q duplicated", gs.Name, ms.ID)
			}
			m := &member{
				id:              ms.ID,
				static:          ms.Static,
				joinedAt:        ms.JoinedAt,
				lastHeartbeatAt: ms.LastHeartbeatAt,
				requests:        make(map[string]requestRecord, len(ms.Requests)),
			}
			if ms.Static {
				inst, ok := g.instances[ms.Instance]
				if !ok {
					return errCorrupt("group %q session %q refers to missing static instance %q",
						gs.Name, ms.ID, ms.Instance)
				}
				if !inst.online {
					return errCorrupt("group %q has active session %q but instance %q is offline",
						gs.Name, ms.ID, ms.Instance)
				}
				if inst.sessionID != ms.ID {
					return errCorrupt("group %q session %q does not match instance %q current session %q",
						gs.Name, ms.ID, ms.Instance, inst.sessionID)
				}
				if inst.version != ms.SessionVersion {
					return errCorrupt("group %q session %q version %d != instance %q version %d",
						gs.Name, ms.ID, ms.SessionVersion, ms.Instance, inst.version)
				}
				m.instance = ms.Instance
				m.sessionVersion = ms.SessionVersion
			} else {
				if ms.Instance != "" || ms.SessionVersion != 0 {
					return errCorrupt("group %q dynamic member %q carries static fields", gs.Name, ms.ID)
				}
				if _, isInstance := g.instances[ms.ID]; isInstance {
					return errCorrupt("group %q id %q is both dynamic member and static instance", gs.Name, ms.ID)
				}
				for _, rs := range ms.Requests {
					m.requests[rs.RequestID] = requestRecord{
						partition: rs.Partition, offset: rs.Offset,
						metadata: rs.Metadata, committedAt: rs.CommittedAt,
					}
				}
			}
			g.members[ms.ID] = m
		}

		// 离线实例不得残留活动会话。
		for id, inst := range g.instances {
			if !inst.online {
				if m, ok := g.members[inst.sessionID]; ok && m.static && m.instance == id {
					return errCorrupt("group %q offline instance %q still has active session %q",
						gs.Name, id, inst.sessionID)
				}
			}
		}

		// 恢复坟场会话。
		for _, ds := range gs.DeadSessions {
			if ds.SessionID == "" {
				return errCorrupt("group %q has dead session with empty session id", gs.Name)
			}
			if _, ok := g.instances[ds.Instance]; !ok {
				return errCorrupt("group %q dead session %q refers to missing instance %q",
					gs.Name, ds.SessionID, ds.Instance)
			}
			if _, active := g.members[ds.SessionID]; active {
				return errCorrupt("group %q session %q is both dead and active", gs.Name, ds.SessionID)
			}
			g.deadSessions[ds.SessionID] = deadSession{instance: ds.Instance, version: ds.Version}
		}

		// 所有权字符串必须指向动态成员或静态实例（空串为无主）。
		validOwner := func(id string) bool {
			if id == "" {
				return true
			}
			if m, ok := g.members[id]; ok {
				return !m.static // 动态成员以自身 ID 持有
			}
			_, ok := g.instances[id] // 静态实例以实例 ID 持有
			return ok
		}
		for p := 0; p < gs.Partitions; p++ {
			if !validOwner(g.assignment.Owners[p]) {
				return errCorrupt("group %q partition %d effective owner %q not a member (neither member nor instance)",
					gs.Name, p, g.assignment.Owners[p])
			}
			if !validOwner(g.target.Owners[p]) {
				return errCorrupt("group %q partition %d target owner %q is neither member nor instance",
					gs.Name, p, g.target.Owners[p])
			}
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
			if len(acked) > 0 {
				g.revokeAcked[rv.MemberID] = acked
			}
		}

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
					continue // 已移交 / 未受影响 / 无主 / 保留期离线钉住
				}
				// 仍由旧主持有的在途分区：旧主必须是现存主体（动态成员，或静态
				// 实例——保留期内离线时其撤销义务挂起，等待重连确认或保留期届满
				// 强制回收），且该分区在其撤销义务中、尚未被确认。
				if _, exists := g.principalLocked(eff); !exists {
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
			for memberID, set := range g.revokeRequired {
				if _, exists := g.principalLocked(memberID); !exists {
					return errCorrupt("group %q revocation obligation belongs to non-member principal %q",
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

		// leader 必须是在线主体（全组仅保留期离线时 leader 允许为空）。
		if g.leader != "" {
			if _, online := g.onlinePrincipalLocked(g.leader); !online {
				return errCorrupt("group %q leader %q is not an online principal", gs.Name, g.leader)
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

func sortInstances(s []StaticInstanceSnapshot) {
	sort.Slice(s, func(i, j int) bool { return s[i].ID < s[j].ID })
}

func sortDeadSessions(s []DeadSessionSnapshot) {
	sort.Slice(s, func(i, j int) bool { return s[i].SessionID < s[j].SessionID })
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
