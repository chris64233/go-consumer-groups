package consumergroups

import (
	"sort"
	"time"
)

// planAssignment 为给定成员集合确定性地产生一整份分区分配（全量轮询）。
//
// 确定性要求：
//   - 成员按 ID 字典序排序，不依赖 map 迭代顺序或到达顺序；
//   - 分区 i 固定归属 members[i % len(members)]，同一版本内每个分区至多
//     归属一个成员，每个成员拿到的分区也互不重叠。
//
// 没有成员时所有分区归属空串（无所有者）。它描述「没有静态成员锚点/清退
// 批次」时的规范分配，planTarget 在纯动态场景下退化为本函数的结果；
// 也供测试按成员集合直接推导期望目标。
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

// planTarget 为新一代版本计算整份目标所有权。
//
// 它是「增量」规划，输入是变更前的生效所有权 baseline 与变更后的成员集合
// g.members（调用方已完成加入/删除）：
//
//   - 保留期内暂时离线的静态成员作为「锚点」：其在 baseline 中持有的分区
//     原样钉住，既不参与轮询重排，也不会被分给其他成员——短暂重连不应丢失
//     分区，离线期间也没有任何进程能替它确认撤销；
//   - 本代被清退（保留期过期/主动退出/管理员移除）的**静态**成员名下分区
//     构成一个整批，**只**转移给同一个新所有者（在线存活者中负载最低、
//     ID 最小者），其余存活成员的既有分区保持不动；
//   - 其余分区（无主分区、动态成员让出的分区、在线成员之间需要重排的分区）
//     按确定性轮询分配给在线成员：分区槽位升序，成员按 ID 字典序。
//
// 当不存在离线静态成员、也没有静态成员清退时，轮询退化为本项目原有的
// 「分区 i 归属 sortedMembers[i % n]」全量分配，因此纯动态成员场景的
// 既有分配结果与旧行为完全一致。
//
// departedStatic 为本代被清退的静态成员 ID（其身份已从 g.members 删除，
// 但 baseline 生效所有权中仍指向它）；为空表示本代没有静态成员清退。
func planTarget(g *group, baseline []string, departedStatic []string, now time.Time) Assignment {
	owners := append([]string(nil), baseline...)
	online := make([]string, 0, len(g.members))
	retained := make(map[string]bool)
	for id, m := range g.members {
		if m.static && !m.online {
			retained[id] = true
		} else {
			online = append(online, id)
		}
	}
	sort.Strings(online)

	departed := make(map[string]bool, len(departedStatic))
	for _, id := range departedStatic {
		departed[id] = true
	}

	// freeSlots：既不被离线静态成员钉住、也不属于某个被清退静态成员整批的槽位，
	// 统一参与在线成员的确定性轮询。
	var freeSlots []int
	for p := 0; p < g.partitions; p++ {
		owner := owners[p]
		if retained[owner] || departed[owner] {
			continue
		}
		freeSlots = append(freeSlots, p)
	}
	if len(online) > 0 {
		for i, p := range freeSlots {
			owners[p] = online[i%len(online)]
		}
	} else {
		for _, p := range freeSlots {
			owners[p] = ""
		}
	}

	// 被清退静态成员的分区按实例聚成整批；每批只选一个新所有者。
	// 批次按离开实例 ID 升序处理，保证同一输入永远产出同一目标。
	batches := make(map[string][]int)
	for _, id := range departedStatic {
		batches[id] = nil
	}
	for p := 0; p < g.partitions; p++ {
		if id := baseline[p]; departed[id] {
			batches[id] = append(batches[id], p)
		}
	}
	batchOwners := make([]string, 0, len(batches))
	for id := range batches {
		batchOwners = append(batchOwners, id)
	}
	sort.Strings(batchOwners)
	for _, id := range batchOwners {
		batch := batches[id]
		if len(batch) == 0 || len(online) == 0 {
			// 没有在线存活者：整批回到无主，等待未来成员加入再分派。
			for _, p := range batch {
				owners[p] = ""
			}
			continue
		}
		dest := pickBatchOwner(owners, online, g.partitions)
		for _, p := range batch {
			owners[p] = dest
		}
	}

	return Assignment{Generation: g.generation, CreatedAt: now, Owners: owners}
}

// pickBatchOwner 在在线存活成员中选出整批分区的唯一新所有者：
// 当前生效所有权下持有分区数最少者，并列时 ID 字典序最小。结果确定。
func pickBatchOwner(owners []string, online []string, partitions int) string {
	load := make(map[string]int, len(online))
	for _, id := range online {
		load[id] = 0
	}
	for p := 0; p < partitions; p++ {
		if _, ok := load[owners[p]]; ok {
			load[owners[p]]++
		}
	}
	choice := online[0]
	for _, id := range online[1:] {
		if load[id] < load[choice] || (load[id] == load[choice] && id < choice) {
			choice = id
		}
	}
	return choice
}

// pickLeader 在**在线**成员中选出 leader：加入最早，加入时刻相同则 ID 较小者。
// 保留期内离线的静态成员不计——它当前没有活跃会话，无法承担 leader 职责；
// 静态成员保留首次加入时间，重连接管后可凭年资重新当选。无在线成员时返回空串。
func pickLeader(members map[string]*member) string {
	var leader string
	var earliest time.Time
	for id, m := range members {
		if m.static && !m.online {
			continue
		}
		if leader == "" ||
			m.joinedAt.Before(earliest) ||
			(m.joinedAt.Equal(earliest) && id < leader) {
			leader, earliest = id, m.joinedAt
		}
	}
	return leader
}
