// Package importer implements utilities used to create DMS3FS DAGs from files
// and readers.
package importer

import (
	bal "github.com/dms3-fs/go-unixfs/importer/balanced"
	h "github.com/dms3-fs/go-unixfs/importer/helpers"
	trickle "github.com/dms3-fs/go-unixfs/importer/trickle"

	chunker "github.com/dms3-fs/go-fs-chunker"
	dms3ld "github.com/dms3-fs/go-ld-format"
)

// BuildDagFromReader creates a DAG given a DAGService and a Splitter
// implementation (Splitters are io.Readers), using a Balanced layout.
func BuildDagFromReader(ds dms3ld.DAGService, spl chunker.Splitter) (dms3ld.Node, error) {
	dbp := h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: h.DefaultLinksPerBlock,
	}

	return bal.Layout(dbp.New(spl))
}

// BuildTrickleDagFromReader creates a DAG given a DAGService and a Splitter
// implementation (Splitters are io.Readers), using a Trickle Layout.
func BuildTrickleDagFromReader(ds dms3ld.DAGService, spl chunker.Splitter) (dms3ld.Node, error) {
	dbp := h.DagBuilderParams{
		Dagserv:  ds,
		Maxlinks: h.DefaultLinksPerBlock,
	}

	return trickle.Layout(dbp.New(spl))
}
