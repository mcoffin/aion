package bucket

import (
	"bytes"
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
