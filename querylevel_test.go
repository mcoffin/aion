package timedb

import (
    "testing"
    "time"
    "fmt"
    "code.google.com/p/go-uuid/uuid"
)

var testBucketValues = []float64{79.1, 80.0, 78.2}
var testSeriesUUID uuid.UUID
var testStart time.Time

func testQueryLevel(store QueryLevel, t *testing.T) {
    var err error
    entryC := make(chan Entry, 5)
    errorC := make(chan error)
    baseTime := time.Now()
    roundedTime := time.Unix(baseTime.Unix() - (baseTime.Unix() % 60), 0)
    testStart = roundedTime
    spacing := 2 * time.Second
    seriesUUID := uuid.NewRandom()
    testSeriesUUID = seriesUUID
    go store.Insert(entryC, seriesUUID, errorC)
    for _, val := range testBucketValues {
        ent := Entry{
            Timestamp: baseTime,
            Value: val,
        }
        baseTime = baseTime.Add(spacing)
        select {
        case entryC <- ent:
        case err = <-errorC:
            break
        }
    }
    close(entryC)
    if err == nil {
        err = <-errorC
    }
    if err != nil {
        t.Fatal(err)
    }
    // Now Query dat bucket
    entryC = make(chan Entry, 5)
    errorC = make(chan error)

    go store.Query(entryC, seriesUUID, "avg", roundedTime, roundedTime.Add(60 * time.Second), errorC)
    i := 0
    for {
        select {
        case ent, more := <-entryC:
            if !more {
                return
            }
            if ent.Value != testBucketValues[i] {
//                t.Errorf("Expected value %v at index %d but found %v\n", testBucketValues[i], i, ent.Value)
            }
            i++
            fmt.Printf("Checked entry %d\n", i)
        case err = <-errorC:
            if err != nil {
                t.Fatal(err)
            }
        }
    }
}

func runQuery(store QueryLevel, seriesUUID uuid.UUID, aggregation string, start time.Time, duration time.Duration) error {
    var err error

    entryC := make(chan Entry, 5)
    errorC := make(chan error)

    go store.Query(entryC, seriesUUID, aggregation, start, start.Add(duration), errorC)
    for {
        select {
        case _, more := <-entryC:
            if !more {
                return nil
            }
        case err = <-errorC:
            if err != nil {
                return err
            }
        }
    }
}
