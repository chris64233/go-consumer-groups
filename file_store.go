package consumergroups

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"time"
)

// FileStore 把协调状态以 JSON 形式持久化到单个文件。
//
// 写入采用「写临时文件 + fsync + 原子 rename」：同一时刻文件内容要么是
// 上一整份快照，要么是新整份快照，不会出现写到一半的半成品状态。
type FileStore struct {
	path string
}

// NewFileStore 创建指向 path 的文件存储（文件可以尚不存在）。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Path 返回持久化文件路径。
func (s *FileStore) Path() string { return s.path }

// fileSnapshot 是 Snapshot 的 JSON 线上格式。
type fileSnapshot struct {
	Version int                 `json:"version"`
	Groups  []fileGroupSnapshot `json:"groups"`
}

type fileGroupSnapshot struct {
	Name             string                   `json:"name"`
	Partitions       int                      `json:"partitions"`
	SessionTimeout   int64                    `json:"session_timeout_ns"`
	Generation       int64                    `json:"generation"`
	Phase            string                   `json:"phase"`
	Leader           string                   `json:"leader"`
	LastRebalance    time.Time                `json:"last_rebalance"`
	Members          []fileMemberSnap         `json:"members"`
	StaticInstances  []fileStaticInstanceSnap `json:"static_instances"`
	DeadSessions     []fileDeadSessionSnap    `json:"dead_sessions"`
	Assignment       fileAssignmentSnap       `json:"assignment"`
	TargetAssignment fileAssignmentSnap       `json:"target_assignment"`
	Revocations      []fileRevocation         `json:"revocations"`
	Offsets          []fileOffsetSnap         `json:"offsets"`
}

type fileStaticInstanceSnap struct {
	ID              string            `json:"id"`
	JoinedAt        time.Time         `json:"joined_at"`
	Retention       int64             `json:"retention_ns"`
	SessionVersion  int64             `json:"session_version"`
	Online          bool              `json:"online"`
	SessionID       string            `json:"session_id"`
	LastHeartbeatAt time.Time         `json:"last_heartbeat_at"`
	OfflineAt       time.Time         `json:"offline_at"`
	RetainUntil     time.Time         `json:"retain_until"`
	Requests        []fileRequestSnap `json:"requests"`
}

type fileDeadSessionSnap struct {
	SessionID string `json:"session_id"`
	Instance  string `json:"instance"`
	Version   int64  `json:"version"`
}

type fileRevocation struct {
	MemberID string `json:"member_id"`
	Required []int  `json:"required"`
	Acked    []int  `json:"acked"`
}

type fileMemberSnap struct {
	ID              string            `json:"id"`
	Static          bool              `json:"static"`
	Instance        string            `json:"instance,omitempty"`
	SessionVersion  int64             `json:"session_version,omitempty"`
	JoinedAt        time.Time         `json:"joined_at"`
	LastHeartbeatAt time.Time         `json:"last_heartbeat_at"`
	Requests        []fileRequestSnap `json:"requests"`
}

type fileRequestSnap struct {
	RequestID   string    `json:"request_id"`
	Partition   int       `json:"partition"`
	Offset      int64     `json:"offset"`
	Metadata    string    `json:"metadata"`
	CommittedAt time.Time `json:"committed_at"`
}

type fileAssignmentSnap struct {
	Generation int64     `json:"generation"`
	CreatedAt  time.Time `json:"created_at"`
	Owners     []string  `json:"owners"`
}

type fileOffsetSnap struct {
	Partition     int       `json:"partition"`
	Offset        int64     `json:"offset"`
	Metadata      string    `json:"metadata"`
	CommittedAt   time.Time `json:"committed_at"`
	LastRequestID string    `json:"last_request_id"`
}

const snapshotFormatVersion = 3

// Save 原子写入整份快照。
func (s *FileStore) Save(snap Snapshot) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	data, err := json.MarshalIndent(toFileSnapshot(&snap), "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".coordinator-state-*")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后 Remove 一个不存在的路径是 no-op

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("atomically replace state file: %w", err)
	}
	return nil
}

// Load 读回最近一次持久化的快照；文件不存在时返回 (nil, nil)。
func (s *FileStore) Load() (*Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read state file: %w", err)
	}
	var fs fileSnapshot
	if err := json.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("decode state file: %w", err)
	}
	if fs.Version != snapshotFormatVersion {
		return nil, errCorrupt("unsupported snapshot version %d", fs.Version)
	}
	return fromFileSnapshot(&fs), nil
}

func toFileSnapshot(s *Snapshot) fileSnapshot {
	out := fileSnapshot{Version: snapshotFormatVersion, Groups: make([]fileGroupSnapshot, 0, len(s.Groups))}
	for _, g := range s.Groups {
		fg := fileGroupSnapshot{
			Name:            g.Name,
			Partitions:      g.Partitions,
			SessionTimeout:  int64(g.SessionTimeout),
			Generation:      g.Generation,
			Phase:           string(g.Phase),
			Leader:          g.Leader,
			LastRebalance:   g.LastRebalance,
			Members:         make([]fileMemberSnap, 0, len(g.Members)),
			StaticInstances: make([]fileStaticInstanceSnap, 0, len(g.StaticInstances)),
			DeadSessions:    make([]fileDeadSessionSnap, 0, len(g.DeadSessions)),
			Assignment: fileAssignmentSnap{
				Generation: g.Assignment.Generation,
				CreatedAt:  g.Assignment.CreatedAt,
				Owners:     append([]string(nil), g.Assignment.Owners...),
			},
			TargetAssignment: fileAssignmentSnap{
				Generation: g.TargetAssignment.Generation,
				CreatedAt:  g.TargetAssignment.CreatedAt,
				Owners:     append([]string(nil), g.TargetAssignment.Owners...),
			},
			Revocations: make([]fileRevocation, 0, len(g.Revocations)),
			Offsets:     make([]fileOffsetSnap, 0, len(g.Offsets)),
		}
		for _, m := range g.Members {
			fm := fileMemberSnap{
				ID:              m.ID,
				Static:          m.Static,
				Instance:        m.Instance,
				SessionVersion:  m.SessionVersion,
				JoinedAt:        m.JoinedAt,
				LastHeartbeatAt: m.LastHeartbeatAt,
				Requests:        make([]fileRequestSnap, 0, len(m.Requests)),
			}
			for _, r := range m.Requests {
				fm.Requests = append(fm.Requests, fileRequestSnap{
					RequestID:   r.RequestID,
					Partition:   r.Partition,
					Offset:      r.Offset,
					Metadata:    r.Metadata,
					CommittedAt: r.CommittedAt,
				})
			}
			fg.Members = append(fg.Members, fm)
		}
		for _, is := range g.StaticInstances {
			fis := fileStaticInstanceSnap{
				ID:              is.ID,
				JoinedAt:        is.JoinedAt,
				Retention:       int64(is.Retention),
				SessionVersion:  is.SessionVersion,
				Online:          is.Online,
				SessionID:       is.SessionID,
				LastHeartbeatAt: is.LastHeartbeatAt,
				OfflineAt:       is.OfflineAt,
				RetainUntil:     is.RetainUntil,
				Requests:        make([]fileRequestSnap, 0, len(is.Requests)),
			}
			for _, r := range is.Requests {
				fis.Requests = append(fis.Requests, fileRequestSnap{
					RequestID:   r.RequestID,
					Partition:   r.Partition,
					Offset:      r.Offset,
					Metadata:    r.Metadata,
					CommittedAt: r.CommittedAt,
				})
			}
			fg.StaticInstances = append(fg.StaticInstances, fis)
		}
		for _, d := range g.DeadSessions {
			fg.DeadSessions = append(fg.DeadSessions, fileDeadSessionSnap{
				SessionID: d.SessionID, Instance: d.Instance, Version: d.Version,
			})
		}
		for _, rv := range g.Revocations {
			fg.Revocations = append(fg.Revocations, fileRevocation{
				MemberID: rv.MemberID,
				Required: append([]int(nil), rv.Required...),
				Acked:    append([]int(nil), rv.Acked...),
			})
		}
		for _, o := range g.Offsets {
			fg.Offsets = append(fg.Offsets, fileOffsetSnap{
				Partition:     o.Partition,
				Offset:        o.Offset,
				Metadata:      o.Metadata,
				CommittedAt:   o.CommittedAt,
				LastRequestID: o.LastRequestID,
			})
		}
		out.Groups = append(out.Groups, fg)
	}
	return out
}

func fromFileSnapshot(fs *fileSnapshot) *Snapshot {
	snap := &Snapshot{Groups: make([]GroupSnapshot, 0, len(fs.Groups))}
	for _, fg := range fs.Groups {
		g := GroupSnapshot{
			Name:            fg.Name,
			Partitions:      fg.Partitions,
			SessionTimeout:  time.Duration(fg.SessionTimeout),
			Generation:      fg.Generation,
			Phase:           RebalancePhase(fg.Phase),
			Leader:          fg.Leader,
			LastRebalance:   fg.LastRebalance,
			Members:         make([]MemberSnapshot, 0, len(fg.Members)),
			StaticInstances: make([]StaticInstanceSnapshot, 0, len(fg.StaticInstances)),
			DeadSessions:    make([]DeadSessionSnapshot, 0, len(fg.DeadSessions)),
			Assignment: Assignment{
				Generation: fg.Assignment.Generation,
				CreatedAt:  fg.Assignment.CreatedAt,
				Owners:     append([]string(nil), fg.Assignment.Owners...),
			},
			TargetAssignment: Assignment{
				Generation: fg.TargetAssignment.Generation,
				CreatedAt:  fg.TargetAssignment.CreatedAt,
				Owners:     append([]string(nil), fg.TargetAssignment.Owners...),
			},
			Revocations: make([]RevocationSnapshot, 0, len(fg.Revocations)),
			Offsets:     make([]Offset, 0, len(fg.Offsets)),
		}
		for _, fm := range fg.Members {
			m := MemberSnapshot{
				ID:              fm.ID,
				Static:          fm.Static,
				Instance:        fm.Instance,
				SessionVersion:  fm.SessionVersion,
				JoinedAt:        fm.JoinedAt,
				LastHeartbeatAt: fm.LastHeartbeatAt,
				Requests:        make([]RequestSnapshot, 0, len(fm.Requests)),
			}
			for _, fr := range fm.Requests {
				m.Requests = append(m.Requests, RequestSnapshot{
					RequestID:   fr.RequestID,
					Partition:   fr.Partition,
					Offset:      fr.Offset,
					Metadata:    fr.Metadata,
					CommittedAt: fr.CommittedAt,
				})
			}
			g.Members = append(g.Members, m)
		}
		for _, fis := range fg.StaticInstances {
			is := StaticInstanceSnapshot{
				ID:              fis.ID,
				JoinedAt:        fis.JoinedAt,
				Retention:       time.Duration(fis.Retention),
				SessionVersion:  fis.SessionVersion,
				Online:          fis.Online,
				SessionID:       fis.SessionID,
				LastHeartbeatAt: fis.LastHeartbeatAt,
				OfflineAt:       fis.OfflineAt,
				RetainUntil:     fis.RetainUntil,
				Requests:        make([]RequestSnapshot, 0, len(fis.Requests)),
			}
			for _, fr := range fis.Requests {
				is.Requests = append(is.Requests, RequestSnapshot{
					RequestID:   fr.RequestID,
					Partition:   fr.Partition,
					Offset:      fr.Offset,
					Metadata:    fr.Metadata,
					CommittedAt: fr.CommittedAt,
				})
			}
			g.StaticInstances = append(g.StaticInstances, is)
		}
		for _, fd := range fg.DeadSessions {
			g.DeadSessions = append(g.DeadSessions, DeadSessionSnapshot{
				SessionID: fd.SessionID, Instance: fd.Instance, Version: fd.Version,
			})
		}
		for _, fr := range fg.Revocations {
			g.Revocations = append(g.Revocations, RevocationSnapshot{
				MemberID: fr.MemberID,
				Required: append([]int(nil), fr.Required...),
				Acked:    append([]int(nil), fr.Acked...),
			})
		}
		for _, fo := range fg.Offsets {
			g.Offsets = append(g.Offsets, Offset{
				Partition:     fo.Partition,
				Offset:        fo.Offset,
				Metadata:      fo.Metadata,
				CommittedAt:   fo.CommittedAt,
				LastRequestID: fo.LastRequestID,
			})
		}
		snap.Groups = append(snap.Groups, g)
	}
	return snap
}

type corruptStateError struct{ msg string }

func (e *corruptStateError) Error() string { return "corrupt coordinator state: " + e.msg }

func errCorrupt(format string, args ...any) error {
	return &corruptStateError{msg: fmt.Sprintf(format, args...)}
}
