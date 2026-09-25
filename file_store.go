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
	Name           string             `json:"name"`
	Partitions     int                `json:"partitions"`
	SessionTimeout int64              `json:"session_timeout_ns"`
	Generation     int64              `json:"generation"`
	Leader         string             `json:"leader"`
	LastRebalance  time.Time          `json:"last_rebalance"`
	Members        []fileMemberSnap   `json:"members"`
	Assignment     fileAssignmentSnap `json:"assignment"`
	Offsets        []fileOffsetSnap   `json:"offsets"`
}

type fileMemberSnap struct {
	ID              string            `json:"id"`
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

const snapshotFormatVersion = 1

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
			Name:           g.Name,
			Partitions:     g.Partitions,
			SessionTimeout: int64(g.SessionTimeout),
			Generation:     g.Generation,
			Leader:         g.Leader,
			LastRebalance:  g.LastRebalance,
			Members:        make([]fileMemberSnap, 0, len(g.Members)),
			Assignment: fileAssignmentSnap{
				Generation: g.Assignment.Generation,
				CreatedAt:  g.Assignment.CreatedAt,
				Owners:     append([]string(nil), g.Assignment.Owners...),
			},
			Offsets: make([]fileOffsetSnap, 0, len(g.Offsets)),
		}
		for _, m := range g.Members {
			fm := fileMemberSnap{
				ID:              m.ID,
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
			Name:           fg.Name,
			Partitions:     fg.Partitions,
			SessionTimeout: time.Duration(fg.SessionTimeout),
			Generation:     fg.Generation,
			Leader:         fg.Leader,
			LastRebalance:  fg.LastRebalance,
			Members:        make([]MemberSnapshot, 0, len(fg.Members)),
			Assignment: Assignment{
				Generation: fg.Assignment.Generation,
				CreatedAt:  fg.Assignment.CreatedAt,
				Owners:     append([]string(nil), fg.Assignment.Owners...),
			},
			Offsets: make([]Offset, 0, len(fg.Offsets)),
		}
		for _, fm := range fg.Members {
			m := MemberSnapshot{
				ID:              fm.ID,
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
