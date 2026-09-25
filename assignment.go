package consumergroups

import "sort"

// computeAssignment 为给定成员集合计算确定性的分区分配：
// 主题按名称排序、成员按 ID 排序后，分区按全局序号轮询分配给成员。
// 相同的成员集合与主题配置必然得到相同的分配结果，
// 且同一版本内一个分区至多归属一个成员。
func computeAssignment(topics map[string]int, memberIDs []string) Assignment {
	assignment := make(Assignment, len(topics))
	if len(memberIDs) == 0 {
		return assignment
	}

	sortedTopics := make([]string, 0, len(topics))
	for name := range topics {
		sortedTopics = append(sortedTopics, name)
	}
	sort.Strings(sortedTopics)

	sortedMembers := append([]string(nil), memberIDs...)
	sort.Strings(sortedMembers)

	base := 0
	for _, topic := range sortedTopics {
		parts := topics[topic]
		owners := make(map[int]string, parts)
		for p := 0; p < parts; p++ {
			owners[p] = sortedMembers[(base+p)%len(sortedMembers)]
		}
		assignment[topic] = owners
		base += parts
	}
	return assignment
}

// ownerOf 返回某分区在当前分配下的所有者，未分配时返回空串。
func ownerOf(a Assignment, topic string, partition int) string {
	if parts, ok := a[topic]; ok {
		return parts[partition]
	}
	return ""
}
