package graph

import (

	"golang.org/x/xerrors"
)

var (
	ErrNotFound = xerrors.New("not found")
	ErrUnknownEdgeLinks = xerrors.New("unknown source and/or destination for the given edge")
)
