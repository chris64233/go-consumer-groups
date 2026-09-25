package consumergroups

import "errors"

// 协调器对外返回的可判别错误。调用方应使用 errors.Is 进行判断，
// 错误信息中会携带具体的组、成员、版本等上下文。
var (
	// ErrGroupExists 表示创建的组已存在。
	ErrGroupExists = errors.New("consumergroups: group already exists")
	// ErrGroupNotFound 表示组不存在。
	ErrGroupNotFound = errors.New("consumergroups: group not found")
	// ErrMemberExists 表示成员已在组内。
	ErrMemberExists = errors.New("consumergroups: member already exists")
	// ErrMemberNotFound 表示成员不在组内（可能已离开或被超时剔除）。
	ErrMemberNotFound = errors.New("consumergroups: member not found")
	// ErrStaleGeneration 表示请求携带的分配版本已过期，
	// 对应的心跳或位点提交来自上一轮分配，必须拒绝。
	ErrStaleGeneration = errors.New("consumergroups: stale generation")
	// ErrNotPartitionOwner 表示提交者不是该分区在当前版本下的所有者。
	ErrNotPartitionOwner = errors.New("consumergroups: not partition owner")
	// ErrOffsetRegression 表示位点后退（小于已提交位点）。
	ErrOffsetRegression = errors.New("consumergroups: offset regression")
	// ErrCommitConflict 表示同一请求号改交了不同的位点。
	ErrCommitConflict = errors.New("consumergroups: commit conflict")
	// ErrUnknownTopic 表示主题不在组订阅范围内。
	ErrUnknownTopic = errors.New("consumergroups: unknown topic")
	// ErrUnknownPartition 表示分区号超出主题的固定分区范围。
	ErrUnknownPartition = errors.New("consumergroups: unknown partition")
	// ErrInvalidRequest 表示请求参数非法（如空请求号、空 ID）。
	ErrInvalidRequest = errors.New("consumergroups: invalid request")
)
