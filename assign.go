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

// principal 是参与分配规划的「主体」：动态成员或静态实例（含保留期内离线者）。
// 分区所有权字符串命名空间里动态成员 ID 与静态实例 ID 全局不重名。
type principal struct {
	id       string
	joinedAt time.Time
	// online 动态成员恒为 true；静态实例保留期内离线时为 false。
	online bool
	// static 是否静态实例。
	static bool
}

// planTarget 计算一个分配版本的目标所有权（下标=分区，值=主体 ID；空串无主）。
//
// 规则：
//   - 保留期内暂时离线的静态实例「钉住」它离线时仍生效持有的分区（retained）：
//     这些分区在它真正退出前不参与再分配，其他成员加入/离开也不动它们；
//   - 其余空闲分区按在线主体（动态成员 + 在线静态实例）ID 字典序轮转分配，
//     按分区升序依次发放（没有钉住分区时与 planAssignment 完全等价）；
//   - evictedBatches 给出本版本被真正清退的静态实例及其原持有分区批次：
//     每一批只能整体移交给唯一新所有者（在线主体中确定性选出的后继），
//     不会被轮转拆散给多个成员；没有在线主体时回到无主（空串）。
//
// 整个函数纯函数、结果确定，不依赖 map 迭代顺序。
func planTarget(
	generation int64,
	partitions int,
	principals []principal,
	retainedOwners map[int]string,
	evictedBatches map[string][]int,
	now time.Time,
) Assignment {
	online := make([]string, 0, len(principals))
	for _, p := range principals {
		if p.online {
			online = append(online, p.id)
		}
	}
	sort.Strings(online)

	owners := make([]string, partitions)

	// 1) 钉住保留期内离线静态实例的分区；清退批次槽位也先排除在轮转之外，
	//    避免其余空闲分区因这些槽位错位，最后再整批指定唯一后继。
	excluded := make(map[int]bool, len(retainedOwners))
	for p, id := range retainedOwners {
		if p >= 0 && p < partitions && id != "" {
			owners[p] = id
			excluded[p] = true
		}
	}
	for _, batch := range evictedBatches {
		for _, p := range batch {
			if p >= 0 && p < partitions {
				excluded[p] = true
			}
		}
	}

	// 2) 其余空闲分区按在线主体轮转（跳过钉住/清退批次槽位）。
	free := 0
	for p := 0; p < partitions; p++ {
		if excluded[p] {
			continue
		}
		if len(online) > 0 {
			owners[p] = online[free%len(online)]
		}
		free++
	}

	// 3) 清退批次整体移交唯一后继（批次不被轮转拆散，始终只给一个新所有者）。
	batchInstances := make([]string, 0, len(evictedBatches))
	for id := range evictedBatches {
		batchInstances = append(batchInstances, id)
	}
	sort.Strings(batchInstances)
	for _, id := range batchInstances {
		successor := pickSuccessor(id, online)
		for _, p := range evictedBatches[id] {
			if p >= 0 && p < partitions {
				owners[p] = successor
			}
		}
	}

	return Assignment{Generation: generation, CreatedAt: now, Owners: owners}
}

// pickSuccessor 在在线主体集合中为被清退实例确定性地选出唯一后继：
// 大于该实例 ID 的最小者，没有则回绕到最小者（环形后继）；集合为空返回空串。
func pickSuccessor(evictedID string, online []string) string {
	for _, id := range online {
		if id > evictedID {
			return id
		}
	}
	if len(online) > 0 {
		return online[0]
	}
	return ""
}

// pickLeaderPrincipals 在全部主体中选出 leader：仅在线主体有资格，
// 加入最早、加入时刻相同则 ID 较小者；无在线主体时返回空串（保留期内
// 全组离线，leader 暂时空缺，任一实例重连后按其首次加入时间接任）。
func pickLeaderPrincipals(principals []principal) string {
	var leader string
	var earliest time.Time
	for _, p := range principals {
		if !p.online {
			continue
		}
		if leader == "" ||
			p.joinedAt.Before(earliest) ||
			(p.joinedAt.Equal(earliest) && p.id < leader) {
			leader, earliest = p.id, p.joinedAt
		}
	}
	return leader
}
