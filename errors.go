package consumergroups

import (
	"errors"
	"fmt"
)

// Sentinel errors. Callers can classify failures with errors.Is, regardless of
// the concrete wrapped type carrying contextual fields.
var (
	// ErrGroupNotFound 组不存在。
	ErrGroupNotFound = errors.New("consumer group not found")
	// ErrGroupAlreadyExists 同名组已存在。
	ErrGroupAlreadyExists = errors.New("consumer group already exists")
	// ErrMemberNotFound 成员不属于当前组（可能已被新版本淘汰）。
	ErrMemberNotFound = errors.New("group member not found")
	// ErrMemberAlreadyExists 成员 ID 已在组内。
	ErrMemberAlreadyExists = errors.New("group member already exists")
	// ErrIllegalGeneration 操作携带的分配版本与当前版本不一致（过旧或超前）。
	// 心跳与位点提交携带旧版本时一律被拒绝。
	ErrIllegalGeneration = errors.New("illegal generation: stale allocation version")
	// ErrNotOwner 成员不是该分区在当前版本下的所有者。
	ErrNotOwner = errors.New("member is not the current partition owner")
	// ErrRequestConflict 同一请求号被用于提交不同的分区或位点。
	ErrRequestConflict = errors.New("idempotency request id conflict")
	// ErrOffsetBacktrack 位点默认单调递增，提交更小的位点会被拒绝。
	ErrOffsetBacktrack = errors.New("offset commit rejected: offset cannot move backwards")
	// ErrInvalidPartition 分区编号越界（主题分区数固定）。
	ErrInvalidPartition = errors.New("invalid partition")
	// ErrInvalidArgument 参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
)

// GenerationMismatchError 在操作携带的分配版本与当前版本不一致时返回。
// errors.Is(err, ErrIllegalGeneration) 成立，同时可通过字段明确判别双方版本。
type GenerationMismatchError struct {
	Group string
	// Want 协调器当前分配版本。
	Want int64
	// Got 请求携带的分配版本。
	Got int64
}

func (e *GenerationMismatchError) Error() string {
	return fmt.Sprintf("%s: group=%q current generation=%d, request generation=%d",
		ErrIllegalGeneration, e.Group, e.Want, e.Got)
}

// Is 让 errors.Is(err, ErrIllegalGeneration) 成立。
func (e *GenerationMismatchError) Is(target error) bool { return target == ErrIllegalGeneration }

// NotPartitionOwnerError 在非当前所有者提交位点时返回。
type NotPartitionOwnerError struct {
	Group     string
	Partition int
	// Owner 当前版本下该分区的所有者；组为空时为空串。
	Owner string
	// Member 发起提交的成员。
	Member string
}

func (e *NotPartitionOwnerError) Error() string {
	return fmt.Sprintf("%s: group=%q partition=%d owner=%q member=%q",
		ErrNotOwner, e.Group, e.Partition, e.Owner, e.Member)
}

func (e *NotPartitionOwnerError) Is(target error) bool { return target == ErrNotOwner }

// RequestConflictError 在同一请求号携带不同内容时返回。
type RequestConflictError struct {
	Group     string
	Member    string
	RequestID string
	// ExistingPartition / ExistingOffset 为该请求号首次提交时记录的内容。
	ExistingPartition int
	ExistingOffset    int64
	// GotPartition / GotOffset 为本次冲突请求携带的内容。
	GotPartition int
	GotOffset    int64
}

func (e *RequestConflictError) Error() string {
	return fmt.Sprintf("%s: group=%q member=%q request=%q existing=(partition=%d, offset=%d) got=(partition=%d, offset=%d)",
		ErrRequestConflict, e.Group, e.Member, e.RequestID,
		e.ExistingPartition, e.ExistingOffset, e.GotPartition, e.GotOffset)
}

func (e *RequestConflictError) Is(target error) bool { return target == ErrRequestConflict }

// OffsetBacktrackError 在提交位点小于已存位点且未显式允许后退时返回。
type OffsetBacktrackError struct {
	Group     string
	Partition int
	// Current 已持久化的较大位点。
	Current int64
	// Requested 本次被拒绝的较小位点。
	Requested int64
}

func (e *OffsetBacktrackError) Error() string {
	return fmt.Sprintf("%s: group=%q partition=%d current=%d requested=%d",
		ErrOffsetBacktrack, e.Group, e.Partition, e.Current, e.Requested)
}

func (e *OffsetBacktrackError) Is(target error) bool { return target == ErrOffsetBacktrack }
