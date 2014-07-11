package bucket

import (
    "bytes"
    "testing"
)

const start = 21
var testVals = []int64{22, 23, 24, 27, 25, 20, 21, 22}

func TestEncodeDecode(t *testing.T) {
    buffer := &bytes.Buffer{}
    enc := NewBucketEncoder(start, buffer)
    enc.Write(testVals)
    enc.Close()
    dec := NewBucketDecoder(start, buffer)
    for i := 0; true; i++ {
        value, err := dec.Read()
        if err != nil {
            if i != len(testVals) {
                t.Errorf("Breaking at index %d instead of expectation %d\n", i, len(testVals))
            }
            break
        }
        if value != testVals[i] {
            t.Errorf("Value %d at index %d doesn't match expectation %d\n", value, i, testVals[i])
        }
    }
}
