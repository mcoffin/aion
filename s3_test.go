package timedb

import (
    "testing"
    "math"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "launchpad.net/goamz/aws"
    "launchpad.net/goamz/s3"
    "fmt"
)


func TestS3BucketStore(t *testing.T) {
    auth, err := aws.EnvAuth()
    if err != nil {
        t.Fatal(err)
    }
    myS3 := s3.New(auth, aws.USWest)
    bucket := myS3.Bucket("timedb-dev")
    store := &S3BucketStore{
        BucketStore{
            Duration: 60 * time.Second,
            Granularity: 0,
            Aggregations: []string{"min", "max", "avg"},
            Multiplier: math.Pow10(1),
        },
        bucket,
    }
    store.Storer = store
    entryC := make(chan Entry, 5)
    errorC := make(chan error)
    baseTime := time.Now()
    roundedTime := time.Unix(baseTime.Unix() - (baseTime.Unix() % 60), 0)
    spacing := 2 * time.Second
    seriesUUID := uuid.NewRandom()
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
                t.Errorf("Expected value %v at index %d but found %v\n", testBucketValues[i], i, ent.Value)
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

func BenchmarkS3Get(b *testing.B) {
    auth, err := aws.EnvAuth()
    if err != nil {
        b.Fatal(err)
    }
    myS3 := s3.New(auth, aws.USWest)
    bucket := myS3.Bucket("timedb-dev")
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := bucket.Get("8d01f8f0-47ea-4cdf-a6b1-52568747dcf4/60/1405616220")
        if err != nil {
            b.Fatal(err)
        }
    }
}
