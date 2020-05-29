package graphtest

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/sbcharr/links/linkgraph/graph"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
	"time"
)

type SuiteBase struct {
	g graph.Graph
}

func (s *SuiteBase) SetGraph(gr graph.Graph) {
	s.g = gr
}

func (s *SuiteBase) TestUpsertLink(c *gc.C) {
	fmt.Println("Running test for:", c.TestName())
	// sub-test-01: create a new link
	original := graph.Link{
		URL:         "https://example.com",
		RetrievedAt: time.Now().Add(-2 * time.Hour).Truncate(time.Second).UTC(),
	}
	err := s.g.UpsertLink(&original)
	// fmt.Printf("address of original struct: %p\n", &original)
	// fmt.Println("original retrieved at:", original.RetrievedAt)
	c.Assert(err, gc.IsNil)
	c.Assert(original.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("sub-test-01: expected a linkID to be assigned to the new link"))

	// sub-test-02: update an existing link and see if its ID changes; ID for an existing link shouldn't change
	accessedAt := time.Now().Truncate(time.Second).UTC()
	existing := graph.Link{
		ID:          original.ID,
		URL:         "https://example.com",
		RetrievedAt: accessedAt,
	}
	// fmt.Printf("address of existing struct: %p\n", &existing)
	err = s.g.UpsertLink(&existing)
	c.Assert(err, gc.IsNil)
	c.Assert(original.ID, gc.Equals, existing.ID, gc.Commentf("sub-test-02: link id changed while upserting"))
	// fmt.Println("original retrieved at after existing:", original.RetrievedAt)
	// fmt.Println("existing retrieved at:", existing.RetrievedAt)

	// sub-test-03: update an existing link with a RetrievedAt date older than the existing date
	sameURL := graph.Link{
		URL: existing.URL,
		RetrievedAt: time.Now().Add(-10 * time.Hour).UTC(),
	}
	err = s.g.UpsertLink(&sameURL)
	c.Assert(err, gc.IsNil)
	c.Assert(sameURL.ID, gc.Equals, existing.ID)

	stored, err := s.g.FindLink(existing.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(stored.RetrievedAt, gc.Equals, accessedAt, gc.Commentf("sub-test-03: last accessed timestamp is updated with an older date"))

	// sub-test-04: create a link with given ID, it should throw error as ID is defined in the db as auto generated
	newID := uuid.New()
	newURL := graph.Link{
		ID: newID,
		URL: "https://examplenew.com",
		RetrievedAt: time.Now().Truncate(time.Second).UTC(),
	}
	err = s.g.UpsertLink(&newURL)
	c.Assert(err, gc.IsNil)
	c.Assert(newURL.ID, gc.Not(gc.Equals), newID, gc.Commentf("sub-test-04: an ID is provided, which should not be the case"))
}

func (s *SuiteBase) TestFindLink(c *gc.C) {
	fmt.Println("Running test for:", c.TestName())
	// create a new link and check if it can be retrieved
	original := graph.Link{
		URL: "https://example.com",
		RetrievedAt: time.Now().Truncate(time.Second).UTC(),
	}
	err := s.g.UpsertLink(&original)
	c.Assert(err, gc.IsNil)

	received, err := s.g.FindLink(original.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(received.ID, gc.Equals, original.ID, gc.Commentf("sub-test-01: ID doesn't match"))
	c.Assert(received.RetrievedAt.UTC(), gc.Equals, original.RetrievedAt, gc.Commentf("sub-test-01: retrievedAt doesn't match"))
}

func (s *SuiteBase) TestUpsertEdge(c *gc.C) {
	fmt.Println("Running test for:", c.TestName())

	// check with a non-existing link
	notExists := graph.Edge{
		Source: uuid.New(),
		Destination: uuid.New(),
	}
	err := s.g.UpsertEdge(&notExists)
	c.Assert(err, gc.Not(gc.IsNil))
	c.Assert(xerrors.Unwrap(err), gc.Equals, graph.ErrUnknownEdgeLinks)

	// create an edge and test its existence
	srcURL := graph.Link{
		URL: "https://srcexample.com",
		RetrievedAt: time.Now().UTC(),
	}
	dstURL := graph.Link{
		URL: "https://dstexample.com",
		RetrievedAt: time.Now().UTC(),
	}
	err = s.g.UpsertLink(&srcURL)
	c.Assert(err, gc.IsNil)

	err = s.g.UpsertLink(&dstURL)
	c.Assert(err, gc.IsNil)

	newEdge := graph.Edge{
		Source: srcURL.ID,
		Destination: dstURL.ID,
	}
	err = s.g.UpsertEdge(&newEdge)
	c.Assert(err, gc.IsNil)
	c.Assert(newEdge.ID, gc.Not(gc.Equals), gc.IsNil, gc.Commentf("sub-test-01: expected a new ID to be assigned to the edge"))
}


func (s *SuiteBase) TestRemoveStaleEdges(c *gc.C) {
	fmt.Println("Running test for:", c.TestName())
	// create a new edge and delete the same
	srcURL := graph.Link{
		URL: "https://srcexample.com",
		RetrievedAt: time.Now().UTC(),
	}
	dstURL := graph.Link{
		URL: "https://dstexample.com",
		RetrievedAt: time.Now().UTC(),
	}
	err := s.g.UpsertLink(&srcURL)
	c.Assert(err, gc.IsNil)

	err = s.g.UpsertLink(&dstURL)
	c.Assert(err, gc.IsNil)

	newEdge := graph.Edge{
		Source: srcURL.ID,
		Destination: dstURL.ID,
	}
	err = s.g.UpsertEdge(&newEdge)
	c.Assert(err, gc.IsNil)
	edgeID := newEdge.ID

	err = s.g.RemoveStaleEdges(newEdge.Source, time.Now().Add(-5 * time.Hour).UTC())
	c.Assert(err, gc.IsNil)
	c.Assert()

}
