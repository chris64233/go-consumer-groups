package consumergroups

import "sync"

// MemoryStore 把快照保存在内存中，主要用于测试或不需要跨进程持久化的场景。
// Save 进行深拷贝，保证已保存快照不会被协调器后续的内部修改所影响。
type MemoryStore struct {
	mu   sync.Mutex
	snap *Snapshot
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Save 整体替换已保存快照。
func (s *MemoryStore) Save(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = cloneSnapshot(&snap)
	return nil
}

// Load 返回已保存快照的深拷贝；从未保存时返回 (nil, nil)。
func (s *MemoryStore) Load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap == nil {
		return nil, nil
	}
	return cloneSnapshot(s.snap), nil
}

// cloneSnapshot 深拷贝快照，隔离协调器内部切片与已持久化数据。
func cloneSnapshot(s *Snapshot) *Snapshot {
	if s == nil {
		return nil
	}
	cp := &Snapshot{Groups: make([]GroupSnapshot, len(s.Groups))}
	for i, g := range s.Groups {
		ng := GroupSnapshot{
			Name:             g.Name,
			Partitions:       g.Partitions,
			SessionTimeout:   g.SessionTimeout,
			Generation:       g.Generation,
			Phase:            g.Phase,
			Leader:           g.Leader,
			LastRebalance:    g.LastRebalance,
			Members:          make([]MemberSnapshot, len(g.Members)),
			StaticInstances:  make([]StaticInstanceSnapshot, len(g.StaticInstances)),
			DeadSessions:     append([]DeadSessionSnapshot(nil), g.DeadSessions...),
			Assignment:       g.Assignment.clone(),
			TargetAssignment: g.TargetAssignment.clone(),
			Revocations:      make([]RevocationSnapshot, len(g.Revocations)),
			Offsets:          append([]Offset(nil), g.Offsets...),
		}
		for j, m := range g.Members {
			nm := MemberSnapshot{
				ID:              m.ID,
				Static:          m.Static,
				Instance:        m.Instance,
				SessionVersion:  m.SessionVersion,
				JoinedAt:        m.JoinedAt,
				LastHeartbeatAt: m.LastHeartbeatAt,
				Requests:        append([]RequestSnapshot(nil), m.Requests...),
			}
			ng.Members[j] = nm
		}
		for j, is := range g.StaticInstances {
			ng.StaticInstances[j] = StaticInstanceSnapshot{
				ID:              is.ID,
				JoinedAt:        is.JoinedAt,
				Retention:       is.Retention,
				SessionVersion:  is.SessionVersion,
				Online:          is.Online,
				SessionID:       is.SessionID,
				LastHeartbeatAt: is.LastHeartbeatAt,
				OfflineAt:       is.OfflineAt,
				RetainUntil:     is.RetainUntil,
				Requests:        append([]RequestSnapshot(nil), is.Requests...),
			}
		}
		for j, rv := range g.Revocations {
			ng.Revocations[j] = RevocationSnapshot{
				MemberID: rv.MemberID,
				Required: append([]int(nil), rv.Required...),
				Acked:    append([]int(nil), rv.Acked...),
			}
		}
		cp.Groups[i] = ng
	}
	return cp
}
