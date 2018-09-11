// Package tar provides functionality to write a unixfs merkledag
// as a tar archive.
package tar

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"path"
	"time"

	mdag "github.com/dms3-fs/go-merkledag"
	ft "github.com/dms3-fs/go-unixfs"
	uio "github.com/dms3-fs/go-unixfs/io"
	upb "github.com/dms3-fs/go-unixfs/pb"

	dms3ld "github.com/dms3-fs/go-ld-format"
)

// Writer is a utility structure that helps to write
// unixfs merkledag nodes as a tar archive format.
// It wraps any io.Writer.
type Writer struct {
	Dag  dms3ld.DAGService
	TarW *tar.Writer

	ctx context.Context
}

// NewWriter wraps given io.Writer.
func NewWriter(ctx context.Context, dag dms3ld.DAGService, w io.Writer) (*Writer, error) {
	return &Writer{
		Dag:  dag,
		TarW: tar.NewWriter(w),
		ctx:  ctx,
	}, nil
}

func (w *Writer) writeDir(nd *mdag.ProtoNode, fpath string) error {
	dir, err := uio.NewDirectoryFromNode(w.Dag, nd)
	if err != nil {
		return err
	}
	if err := writeDirHeader(w.TarW, fpath); err != nil {
		return err
	}

	return dir.ForEachLink(w.ctx, func(l *dms3ld.Link) error {
		child, err := w.Dag.Get(w.ctx, l.Cid)
		if err != nil {
			return err
		}
		npath := path.Join(fpath, l.Name)
		return w.WriteNode(child, npath)
	})
}

func (w *Writer) writeFile(nd *mdag.ProtoNode, fsNode *ft.FSNode, fpath string) error {
	if err := writeFileHeader(w.TarW, fpath, fsNode.FileSize()); err != nil {
		return err
	}

	dagr := uio.NewPBFileReader(w.ctx, nd, fsNode, w.Dag)
	if _, err := dagr.WriteTo(w.TarW); err != nil {
		return err
	}
	w.TarW.Flush()
	return nil
}

// WriteNode adds a node to the archive.
func (w *Writer) WriteNode(nd dms3ld.Node, fpath string) error {
	switch nd := nd.(type) {
	case *mdag.ProtoNode:
		fsNode, err := ft.FSNodeFromBytes(nd.Data())
		if err != nil {
			return err
		}

		switch fsNode.Type() {
		case upb.Data_Metadata:
			fallthrough
		case upb.Data_Directory, upb.Data_HAMTShard:
			return w.writeDir(nd, fpath)
		case upb.Data_Raw:
			fallthrough
		case upb.Data_File:
			return w.writeFile(nd, fsNode, fpath)
		case upb.Data_Symlink:
			return writeSymlinkHeader(w.TarW, string(fsNode.Data()), fpath)
		default:
			return ft.ErrUnrecognizedType
		}
	case *mdag.RawNode:
		if err := writeFileHeader(w.TarW, fpath, uint64(len(nd.RawData()))); err != nil {
			return err
		}

		if _, err := w.TarW.Write(nd.RawData()); err != nil {
			return err
		}
		w.TarW.Flush()
		return nil
	default:
		return fmt.Errorf("nodes of type %T are not supported in unixfs", nd)
	}
}

// Close closes the tar writer.
func (w *Writer) Close() error {
	return w.TarW.Close()
}

func writeDirHeader(w *tar.Writer, fpath string) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Typeflag: tar.TypeDir,
		Mode:     0777,
		ModTime:  time.Now(),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}

func writeFileHeader(w *tar.Writer, fpath string, size uint64) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Size:     int64(size),
		Typeflag: tar.TypeReg,
		Mode:     0644,
		ModTime:  time.Now(),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}

func writeSymlinkHeader(w *tar.Writer, target, fpath string) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Linkname: target,
		Mode:     0777,
		Typeflag: tar.TypeSymlink,
	})
}
