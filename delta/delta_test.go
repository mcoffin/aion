package delta

import (
    "fmt"
    "testing"
)

var testVals = []float64{1.1, 2.1, -2.3, 4.7, 3.5}

func TestEncodeDecode(t *testing.T) {
    enc := NewEncoder(1.0, 1)
    encoded := enc.Encode(testVals)
    if len(encoded) != len(testVals) {
        t.Fatalf("Expected %d encoded items, got %d\n", len(testVals), len(encoded))
    }
    dec := NewDecoder(1.0, 1)
    decoded := dec.Decode(encoded)
    if len(decoded) != len(testVals) {
        t.Fatalf("Expected %d decoded items, got %d\n", len(testVals), len(decoded))
    }
    for i, v := range decoded {
        fmt.Println(v)
        if v != testVals[i] {
            t.Errorf("Decoded value %f from encoded value %d at index %d doesnt match expected value %f\n", v, encoded[i], i, testVals[i])
        }
    }
}
