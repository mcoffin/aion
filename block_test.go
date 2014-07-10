package timedb

import (
    "math/rand"
    "testing"
    "time"
)

const (
    dataPoints = 60
    maxVariation = 50
    start = 4.2
    precision = 1
)

func TestBlock(t *testing.T) {
    entries := make([]Entry, dataPoints)
    dummyTime := time.Now()
    entries[0] = Entry{
        Value: start,
        Time: dummyTime,
    }
    for i := 1; i < len(entries); i++ {
        entries[i] = Entry{
            Value: entries[i - 1].Value + ((rand.Float64() - 0.5) * maxVariation),
            Time: dummyTime,
        }
    }
    block := NewBlock(dummyTime, precision, [][]Entry{entries})
    dec := block.CreateBucketDecoder(0)
    for i, entry := range entries {
        value, err := dec.ReadFloat64()
        if err != nil {
            t.Fatal(err)
        }
        if value != entry.Value {
            t.Errorf("Value %f at index %d doesn't match expected value %f\n", value, i, entry.Value)
        }
    }
}
