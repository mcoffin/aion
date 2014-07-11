package timedb

import (
    "math/rand"
    "testing"
    "time"
)

const (
    dataPoints = 5
    granularity = "1m"
    maxVariation = 50.0
    start = 4.2
    precision = 1
)

func TestEncodeDecode(t *testing.T) {
    d, err := time.ParseDuration(granularity)
    if err != nil {
        t.Fatal(err)
    }
    data := make([]Entry, dataPoints)
    data[0] = Entry{
        Value: start,
        Time: time.Now(),
    }
    for i := 1; i < len(data); i++ {
        data[i] = Entry{
            Value: data[i-1].Value + ((rand.Float64() - 0.5) * maxVariation),
            Time: data[i-1].Time.Add(d),
        }
    }
    block := NewBlock(data[0].Time, precision, [][]Entry{data})
    dec := block.CreateBlockDecoder(0)
    for i := 0; true; i++ {
        entry, err := dec.Read()
        if err != nil {
            if i != len(data) {
                t.Fatalf("Returned %d entries but expected %d\n", i, len(data))
            }
            break
        }
        if entry.Value != data[i].Value {
            t.Errorf("Value %f at index %d differs from expectation %f\n", entry.Value, i, data[i].Value)
        }
        if entry.Time.Unix() != data[i].Time.Unix() {
            t.Errorf("Time %v at index %d differs from expectation %v\n", entry.Time.Unix(), i, data[i].Time.Unix())
        }
    }
}
