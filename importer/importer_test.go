package importer

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	uio "github.com/dms3-fs/go-unixfs/io"

	cid "github.com/dms3-fs/go-cid"
	chunker "github.com/dms3-fs/go-fs-chunker"
	u "github.com/dms3-fs/go-fs-util"
	dms3ld "github.com/dms3-fs/go-ld-format"
	mdtest "github.com/dms3-fs/go-merkledag/test"
)

func getBalancedDag(t testing.TB, size int64, blksize int64) (dms3ld.Node, dms3ld.DAGService) {
	ds := mdtest.Mock()
	r := io.LimitReader(u.NewTimeSeededRand(), size)
	nd, err := BuildDagFromReader(ds, chunker.NewSizeSplitter(r, blksize))
	if err != nil {
		t.Fatal(err)
	}
	return nd, ds
}

func getTrickleDag(t testing.TB, size int64, blksize int64) (dms3ld.Node, dms3ld.DAGService) {
	ds := mdtest.Mock()
	r := io.LimitReader(u.NewTimeSeededRand(), size)
	nd, err := BuildTrickleDagFromReader(ds, chunker.NewSizeSplitter(r, blksize))
	if err != nil {
		t.Fatal(err)
	}
	return nd, ds
}

func TestStableCid(t *testing.T) {
	ds := mdtest.Mock()
	buf := make([]byte, 10*1024*1024)
	u.NewSeededRand(0xdeadbeef).Read(buf)
	r := bytes.NewReader(buf)

	nd, err := BuildDagFromReader(ds, chunker.DefaultSplitter(r))
	if err != nil {
		t.Fatal(err)
	}

	expected, err := cid.Decode("QmZN1qquw84zhV4j6vT56tCcmFxaDaySL1ezTXFvMdNmrK")
	if err != nil {
		t.Fatal(err)
	}
	if !expected.Equals(nd.Cid()) {
		t.Fatalf("expected CID %s, got CID %s", expected, nd)
	}

	dr, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(dr)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out, buf) {
		t.Fatal("bad read")
	}
}

func TestBalancedDag(t *testing.T) {
	ds := mdtest.Mock()
	buf := make([]byte, 10000)
	u.NewTimeSeededRand().Read(buf)
	r := bytes.NewReader(buf)

	nd, err := BuildDagFromReader(ds, chunker.DefaultSplitter(r))
	if err != nil {
		t.Fatal(err)
	}

	dr, err := uio.NewDagReader(context.Background(), nd, ds)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ioutil.ReadAll(dr)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out, buf) {
		t.Fatal("bad read")
	}
}

func BenchmarkBalancedReadSmallBlock(b *testing.B) {
	b.StopTimer()
	nbytes := int64(10000000)
	nd, ds := getBalancedDag(b, nbytes, 4096)

	b.SetBytes(nbytes)
	b.StartTimer()
	runReadBench(b, nd, ds)
}

func BenchmarkTrickleReadSmallBlock(b *testing.B) {
	b.StopTimer()
	nbytes := int64(10000000)
	nd, ds := getTrickleDag(b, nbytes, 4096)

	b.SetBytes(nbytes)
	b.StartTimer()
	runReadBench(b, nd, ds)
}

func BenchmarkBalancedReadFull(b *testing.B) {
	b.StopTimer()
	nbytes := int64(10000000)
	nd, ds := getBalancedDag(b, nbytes, chunker.DefaultBlockSize)

	b.SetBytes(nbytes)
	b.StartTimer()
	runReadBench(b, nd, ds)
}

func BenchmarkTrickleReadFull(b *testing.B) {
	b.StopTimer()
	nbytes := int64(10000000)
	nd, ds := getTrickleDag(b, nbytes, chunker.DefaultBlockSize)

	b.SetBytes(nbytes)
	b.StartTimer()
	runReadBench(b, nd, ds)
}

func runReadBench(b *testing.B, nd dms3ld.Node, ds dms3ld.DAGService) {
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		read, err := uio.NewDagReader(ctx, nd, ds)
		if err != nil {
			b.Fatal(err)
		}

		_, err = read.WriteTo(ioutil.Discard)
		if err != nil && err != io.EOF {
			b.Fatal(err)
		}
		cancel()
	}
}
