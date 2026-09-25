package consumergroups

import "time"

// TopicConfig 描述组订阅的一个主题及其固定分区数。
type TopicConfig struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

// Assignment 是一次再均衡的完整结果：topic -> partition -> memberID。
// 同一版本内一个分区至多归属一个成员。
type Assignment map[string]map[int]string

// MemberState 是成员的对外可见状态。
type MemberState struct {
	ID            string    `json:"id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// OffsetState 是一个分区已提交的位点及最近一次提交的请求号。
type OffsetState struct {
	Offset    int64  `json:"offset"`
	RequestID string `json:"request_id"`
}

// GroupState 是组协调状态的整体快照，供状态查询使用。
type GroupState struct {
	ID         string                         `json:"id"`
	Generation int64                          `json:"generation"`
	Topics     map[string]int                 `json:"topics"`
	Members    []MemberState                  `json:"members"`
	Assignment Assignment                     `json:"assignment"`
	Offsets    map[string]map[int]OffsetState `json:"offsets"`
}

// member 是协调器内部的成员记录。
type member struct {
	id            string
	lastHeartbeat time.Time
}

// group 是协调器内部的组记录。
type group struct {
	id         string
	topics     map[string]int
	generation int64
	members    map[string]*member
	assignment Assignment
	offsets    map[string]map[int]OffsetState
}
