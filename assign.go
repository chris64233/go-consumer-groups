package consumergroups

import (
	"sort"
	"time"
)

// planAssignment 为给定成员集合确定性地产生一整份分区分配。
//
// 确定性要求：
//   - 成员按 ID 字典序排序，不依赖 map 迭代顺序或到达顺序；
//   - 分区 i 固定归属 members[i % len(members)]，同一版本内每个分区至多
//     归属一个成员，每个成员拿到的分区也互不重叠。
//
// 没有成员时所有分区归属空串（无所有者）。
func planAssignment(generation int64, partitions int, members map[string]*member, now time.Time) Assignment {
	owners := make([]string, partitions)
	if len(members) == 0 {
		return Assignment{Generation: generation, CreatedAt: now, Owners: owners}
	}

	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for p := 0; p < partitions; p++ {
		owners[p] = ids[p%len(ids)]
	}
	return Assignment{Generation: generation, CreatedAt: now, Owners: owners}
}

// pickLeader 在成员集合中选出 leader：加入最早，加入时刻相同则 ID 较小者。
// 结果同样是确定性的。无成员时返回空串。
func pickLeader(members map[string]*member) string {
	var leader string
	var earliest time.Time
	for id, m := range members {
		if leader == "" ||
			m.joinedAt.Before(earliest) ||
			(m.joinedAt.Equal(earliest) && id < leader) {
			leader, earliest = id, m.joinedAt
		}
	}
	return leader
}
