package bucket

import (
	"bytes"
	"io"
	"testing"
)

const start = 21

var testVals = []int64{22, 23, 24, 27, 25, 20, 21, 22}
var testVals2 = []int64{18, 16, 28, 31, 33, 26, 26, 25}
var testBucketData = [][]int64{testVals, testVals2}

func TestEncodeDecode(t *testing.T) {
	buffer := &bytes.Buffer{}
	enc := NewBucketEncoder(start, buffer)
	enc.Write(testVals)
	enc.Close()
	dec := NewBucketDecoder(start, buffer)
	decoded := make([]int64, len(testVals))
	n, err := dec.Read(decoded)
	if err != err {
		t.Fatal(err)
	}
	if n != len(testVals) {
		t.Fatalf("Read %d values instead of expectation %d\n", n, len(testVals))
	}
	for i, decodedValue := range decoded {
		if decodedValue != testVals[i] {
			t.Errorf("Decoded value %d at index %d doesn't match expectation %d\n", decodedValue, i, testVals[i])
		}
	}
}

func TestFlush(t *testing.T) {
	var buf bytes.Buffer
	enc := NewBucketEncoder(0, &buf)
	enc.WriteInt(31)
	enc.WriteInt(32)
	enc.FlushBuffer()
	decBuf := bytes.NewBuffer(buf.Bytes())
	enc.Flush(decBuf)
	dec := NewBucketDecoder(0, decBuf)
	decoded := make([]int64, 2)
	n, err := dec.Read(decoded)
	if err != nil && err.Error() != io.EOF.Error() {
		t.Fatalf("Read %d items before error: %v", n, err)
	}
	if n != 2 {
		t.Fatalf("Decoded %d values instead of 2", n)
	}
}

func TestFlushLargeNumbers(t *testing.T) {
	largeValues := []int64{1409337152104649, 1409337156864482, 1409337159111547, 1409337161269084}
	var buf bytes.Buffer
	enc := NewBucketEncoder(1409336945224193, &buf)
	enc.Write(largeValues)
	enc.FlushBuffer()
	decBuf := bytes.NewBuffer(buf.Bytes())
	enc.Flush(decBuf)
	dec := NewBucketDecoder(1409336945224193, decBuf)
	decoded := make([]int64, 1)
	overall := 0
	for {
		n, err := dec.Read(decoded)
		if n > 0 {
			overall++
		}
		if err != nil {
			break
		}
	}
	if overall != len(largeValues) {
		t.Errorf("Read %d items instead of %d", overall, len(largeValues))
	}
}
