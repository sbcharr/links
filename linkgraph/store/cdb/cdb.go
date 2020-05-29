package cdb

import (
	"database/sql"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"golang.org/x/xerrors"
	"time"

	_ "github.com/lib/pq"
	"github.com/sbcharr/links/linkgraph/graph"
)

var (
	upsertLinkQuery = `
INSERT INTO links(url, retrieved_at) VALUES($1, $2)
ON CONFLICT (url) DO UPDATE SET retrieved_at=GREATEST(links.retrieved_at, $2)
RETURNING id, retrieved_at;`

	findLinkQuery = `SELECT url, retrieved_at FROM links WHERE id=$1;`

	upsertEdgeQuery = `
INSERT INTO edges(src, dst, updated_at) VALUES($1, $2, NOW())
ON CONFLICT (src, dst) DO UPDATE SET updated_at=NOW()
RETURNING id, updated_at;`

	removeStaleEdgeQuery = `DELETE FROM edges WHERE src = $1 AND updated_at < $2;`
	// compile-time check for ensuring CockroachDBGraph implements Graph
	_ graph.Graph = (*CockroachDBGraph)(nil)
)

type CockroachDBGraph struct {
	db *sql.DB
}

func NewCockroachDBGraph(dsn string) (*CockroachDBGraph, error) {
	db , err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return &CockroachDBGraph{db: db}, nil
}

func (c *CockroachDBGraph) Close() error {
	return c.db.Close()
}

func (c *CockroachDBGraph) UpsertLink(link *graph.Link) error {
	row := c.db.QueryRow(upsertLinkQuery, link.URL, link.RetrievedAt.UTC())
	if err := row.Scan(&link.ID, &link.RetrievedAt); err != nil {
		return xerrors.Errorf("upsert link: %w", err)
	}
	link.RetrievedAt = link.RetrievedAt.UTC()

	return nil
}

func (c *CockroachDBGraph) FindLink(id uuid.UUID) (*graph.Link, error) {
	row := c.db.QueryRow(findLinkQuery, id)
	link := &graph.Link{ID: id}
	if err := row.Scan(&link.URL, &link.RetrievedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, xerrors.Errorf("find link: %w", graph.ErrNotFound)
		}
		return nil, xerrors.Errorf("find link: %w", err)
	}
	link.RetrievedAt = link.RetrievedAt.UTC()

	return link, nil
}

func (c *CockroachDBGraph) UpsertEdge(edge *graph.Edge) error {
	row := c.db.QueryRow(upsertEdgeQuery, edge.Source, edge.Destination)
	if err := row.Scan(&edge.ID, &edge.UpdatedAt); err != nil {
		if isForeignKeyValidationError(err) {
			return xerrors.Errorf("upsert edge: %w", graph.ErrUnknownEdgeLinks)
		}
		return xerrors.Errorf("upsert edge: %w", err)
	}
	edge.UpdatedAt = edge.UpdatedAt.UTC()

	return nil
}

func (c *CockroachDBGraph) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error {
	_, err := c.db.Exec(removeStaleEdgeQuery, fromID, updatedBefore.UTC())
	if err != nil {
		return xerrors.Errorf("remove stale edges: %w", err)
	}

	return nil
}

func isForeignKeyValidationError(err error) bool {
	pqError, ok := err.(*pq.Error)
	if !ok {
		return false
	}

	return pqError.Code.Name() == "foreign_key_violation"
}

