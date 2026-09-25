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
			Name:           g.Name,
			Partitions:     g.Partitions,
			SessionTimeout: g.SessionTimeout,
			Generation:     g.Generation,
			Leader:         g.Leader,
			LastRebalance:  g.LastRebalance,
			Members:        make([]MemberSnapshot, len(g.Members)),
			Assignment:     g.Assignment.clone(),
			Offsets:        append([]Offset(nil), g.Offsets...),
		}
		for j, m := range g.Members {
			nm := MemberSnapshot{
				ID:              m.ID,
				JoinedAt:        m.JoinedAt,
				LastHeartbeatAt: m.LastHeartbeatAt,
				Requests:        append([]RequestSnapshot(nil), m.Requests...),
			}
			ng.Members[j] = nm
		}
		cp.Groups[i] = ng
	}
	return cp
}
