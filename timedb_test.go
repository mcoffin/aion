package timedb

import (
    "math/rand"
    "testing"
    "time"
    "log"
)

const (
    dataPoints = 60
    maxVariation = 50.0
)

func TestBlockify(t *testing.T) {
    dummyTime := time.Now()
    bucketData := make(BucketData, dataPoints)
    for i := range bucketData {
        bucketData[i].Value = Value(maxVariation * rand.Float32())
    }
    data := BlockData{
        bucketData,
    }
    enc, err := data.Blockify(dummyTime, 1)
    log.Printf("Encoded %d data points with variation [0.0, %.1f) to %d bytes\n", dataPoints, maxVariation, len(enc.buckets[0]))
    if err != nil {
        t.Error(err)
    }
}

var benchvals = BucketData{
    Entry{
        Value: 31.0,
        Time: time.Now(),
    },
    Entry{
        Value: 32.6,
        Time: time.Now(),
    },
    Entry{
        Value: 29.5,
        Time: time.Now(),
    },
}

var benchStart = Entry{
    Value: 31.0,
    Time: time.Now(),
}

func BenchmarkBucketize(b *testing.B) {
    for i := 0; i < b.N; i++ {
        benchvals.bucketize(benchStart, 1)
    }
}

func BenchmarkBlockify(b *testing.B) {
    b.StopTimer()
    dummyTime := time.Now()
    data := BlockData{benchvals}
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        _, err := data.Blockify(dummyTime, 1)
        if err != nil {
            b.Error(err)
        }
    }
}
