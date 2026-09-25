package consumergroups

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Storage 是协调状态的持久化接口。协调器在每次状态变更后
// 将整体快照交给 Storage 保存，启动时通过 Load 恢复。
type Storage interface {
	// Save 原子地保存一份快照。
	Save(snapshot []byte) error
	// Load 读取最近一次保存的快照；没有任何快照时返回 nil, nil。
	Load() ([]byte, error)
}

// FileStorage 将快照以 JSON 文件形式保存，写入时先写临时文件
// 再 rename，保证不会留下半份文件。
type FileStorage struct {
	path string
}

// NewFileStorage 创建一个以 path 为快照文件的持久化存储。
func NewFileStorage(path string) *FileStorage {
	return &FileStorage{path: path}
}

func (s *FileStorage) Save(snapshot []byte) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("consumergroups: create storage dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("consumergroups: create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(snapshot); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("consumergroups: write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("consumergroups: sync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("consumergroups: close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("consumergroups: rename snapshot: %w", err)
	}
	return nil
}

func (s *FileStorage) Load() ([]byte, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("consumergroups: read snapshot: %w", err)
	}
	return data, nil
}

// snapshot 是持久化的顶层结构。
type snapshot struct {
	Groups map[string]groupSnapshot `json:"groups"`
}

type groupSnapshot struct {
	ID         string                         `json:"id"`
	Topics     map[string]int                 `json:"topics"`
	Generation int64                          `json:"generation"`
	Members    map[string]memberSnapshot      `json:"members"`
	Assignment Assignment                     `json:"assignment"`
	Offsets    map[string]map[int]OffsetState `json:"offsets"`
}

type memberSnapshot struct {
	ID            string    `json:"id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

func encodeSnapshot(groups map[string]*group) ([]byte, error) {
	snap := snapshot{Groups: make(map[string]groupSnapshot, len(groups))}
	for id, g := range groups {
		gs := groupSnapshot{
			ID:         g.id,
			Topics:     g.topics,
			Generation: g.generation,
			Members:    make(map[string]memberSnapshot, len(g.members)),
			Assignment: g.assignment,
			Offsets:    g.offsets,
		}
		for mid, m := range g.members {
			gs.Members[mid] = memberSnapshot{ID: m.id, LastHeartbeat: m.lastHeartbeat}
		}
		snap.Groups[id] = gs
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("consumergroups: encode snapshot: %w", err)
	}
	return data, nil
}

func decodeSnapshot(data []byte) (map[string]*group, error) {
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("consumergroups: decode snapshot: %w", err)
	}
	groups := make(map[string]*group, len(snap.Groups))
	for id, gs := range snap.Groups {
		g := &group{
			id:         gs.ID,
			topics:     gs.Topics,
			generation: gs.Generation,
			members:    make(map[string]*member, len(gs.Members)),
			assignment: gs.Assignment,
			offsets:    gs.Offsets,
		}
		if g.assignment == nil {
			g.assignment = Assignment{}
		}
		if g.offsets == nil {
			g.offsets = map[string]map[int]OffsetState{}
		}
		for mid, ms := range gs.Members {
			g.members[mid] = &member{id: ms.ID, lastHeartbeat: ms.LastHeartbeat}
		}
		groups[id] = g
	}
	return groups, nil
}
