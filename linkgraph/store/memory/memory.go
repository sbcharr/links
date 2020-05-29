package memory

import (
	"github.com/google/uuid"
	//"github.com/sbcharr/links/linkgraph/graph"
	//"sync"
)

type edgeList []uuid.UUID
/*
type InMemoryGraph struct {
	mu sync.RWMutex

	links map[uuid.UUID]*graph.Link
	edges map[uuid.UUID]*graph.Edge

	linkURLIndex map[string]*graph.Link
	linkEdgeMap map[uuid.UUID]edgeList
}

func NewInMemoryGraph() *InMemoryGraph {
	return &InMemoryGraph{
		links: make(map[uuid.UUID]*graph.Link),
		edges: make(map[uuid.UUID]*graph.Edge),
		linkURLIndex: make(map[string]*graph.Link),
		linkEdgeMap: make(map[uuid.UUID]edgeList),
	}
}

func (m *InMemoryGraph) UpsertLink(link *graph.Link) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing := m.linkURLIndex[link.URL]; existing != nil {
		link.ID = existing.ID
		origTs := existing.RetrievedAt
		*existing = *link
		if origTs.After(existing.RetrievedAt) {
			existing.RetrievedAt = origTs
		}
		return nil
	}

	// Assign new ID and ensure it's not a duplicate
	for {
		link.ID = uuid.New()
		if m.links[link.ID] == nil {
			break
		}
	}
	lcopy := new(graph.Link)
	*lcopy = *link
	m.linkURLIndex[lcopy.URL] = lcopy
	m.links[lcopy.ID] = lcopy

	return nil
}
*/