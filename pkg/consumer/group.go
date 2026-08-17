package consumer

import (
	"fmt"
	"sync"
	"time"
)

// Member represents an active consumer inside a consumer group.
type Member struct {
	ID            string
	ClientHost    string
	AssignedParts []int
	LastHeartbeat time.Time
}

// Group manages consumer subscriptions, heartbeat checks, and offset commitments.
type Group struct {
	mu               sync.RWMutex
	name             string
	topic            string
	members          map[string]*Member
	committedOffsets map[int]uint64 // partitionID -> committedOffset
	heartbeatTimeout time.Duration
}

// NewGroup creates a new consumer group.
func NewGroup(name, topic string) *Group {
	return &Group{
		name:             name,
		topic:            topic,
		members:          make(map[string]*Member),
		committedOffsets: make(map[int]uint64),
		heartbeatTimeout: 10 * time.Second,
	}
}

// Join registers a member in the group and triggers partition rebalancing.
func (g *Group) Join(memberID, host string, totalPartitions int) ([]int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.members[memberID] = &Member{
		ID:            memberID,
		ClientHost:    host,
		LastHeartbeat: time.Now(),
	}

	return g.rebalance(totalPartitions), nil
}

// Heartbeat refreshes member liveness.
func (g *Group) Heartbeat(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	member, exists := g.members[memberID]
	if !exists {
		return fmt.Errorf("member %s not registered in group %s", memberID, g.name)
	}
	member.LastHeartbeat = time.Now()
	return nil
}

// Leave unregisters a member and triggers rebalance.
func (g *Group) Leave(memberID string, totalPartitions int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.members, memberID)
	g.rebalance(totalPartitions)
}

// CommitOffset updates the committed position for a partition.
func (g *Group) CommitOffset(partition int, offset uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.committedOffsets[partition] = offset
}

// GetCommittedOffset returns the last committed offset for a partition.
func (g *Group) GetCommittedOffset(partition int) uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.committedOffsets[partition]
}

// Internal rebalance algorithm (Round-robin assignment of partitions across active members)
func (g *Group) rebalance(totalPartitions int) []int {
	memberList := make([]*Member, 0, len(g.members))
	for _, m := range g.members {
		m.AssignedParts = nil
		memberList = append(memberList, m)
	}

	if len(memberList) == 0 {
		return nil
	}

	for p := 0; p < totalPartitions; p++ {
		targetMember := memberList[p%len(memberList)]
		targetMember.AssignedParts = append(targetMember.AssignedParts, p)
	}

	return memberList[0].AssignedParts
}

// Status returns snapshot information for monitoring.
func (g *Group) Status() map[string]interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()

	memberSummaries := make([]map[string]interface{}, 0, len(g.members))
	for _, m := range g.members {
		memberSummaries = append(memberSummaries, map[string]interface{}{
			"id":         m.ID,
			"host":       m.ClientHost,
			"partitions": m.AssignedParts,
			"heartbeat":  m.LastHeartbeat.Format(time.RFC3339),
		})
	}

	return map[string]interface{}{
		"group":             g.name,
		"topic":             g.topic,
		"members_count":     len(g.members),
		"members":           memberSummaries,
		"committed_offsets": g.committedOffsets,
	}
}
