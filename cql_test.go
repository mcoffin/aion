package timedb

import (
    "testing"
    "math"
    "time"
    "fmt"
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
)

const (
    clusterIp = "172.28.128.2"
    keyspace = "timedb"
)

func TestCQLCacheInsert(t *testing.T) {
    cluster := gocql.NewCluster(clusterIp)
    cluster.Keyspace = keyspace
    session, err := cluster.CreateSession()
    if err != nil {
        t.Fatal(err)
    }
    cache := &CQLCache{
        Session: session,
    }
    tdb := NewTimeDB(cache)
    err = tdb.Put(uuid.NewRandom(), 79.1, time.Now())
    if err != nil {
        t.Error(err)
    }
}

const (
    queryBufSize = 5
    series = "e44de0f9-e4f4-4fe9-8445-87b6e6ce6f1c"
)

func TestCQLCacheQuery(t *testing.T) {
    cluster := gocql.NewCluster(clusterIp)
    cluster.Keyspace = keyspace
    session, err := cluster.CreateSession()
    defer session.Close()
    if err != nil {
        t.Fatal(err)
    }
    cache := &CQLCache{
        Session: session,
    }
    entryC := make(chan Entry, 5)
    errorC := make(chan error)
    seriesUUID := uuid.Parse(series)
    start := time.Date(2014, time.January, 1, 0, 0, 0, 0, time.Local)
    duration, err := time.ParseDuration("8760h")
    end := start.Add(duration)
    go cache.Query(entryC, seriesUUID, 0, "raw", start, end, errorC)
    for {
        entry, more := <-entryC
        if more {
            fmt.Println(entry)
        } else {
            break
        }
    }
    err = <-errorC
    if err != nil {
        t.Error(err)
    }
}

var testBucketValues = []float64{79.1, 80.0, 78.2}

func TestDateRounding(t *testing.T) {
    d := time.Now()
    store := &BucketStore{
        Duration: 60 * time.Second,
    }
    rounded := store.nearestStart(d)
    if rounded.Second() != 0 {
        t.Errorf("Expected rounded date with 0 seconds but found %v\n", rounded)
    }
}

func TestCQLBucketStore(t *testing.T) {
    cluster := gocql.NewCluster(clusterIp)
    cluster.Keyspace = keyspace
    session, err := cluster.CreateSession()
    defer session.Close()
    if err != nil {
        t.Fatal(err)
    }
    bDuration, _ := time.ParseDuration("60s")
    store := &CQLBucketStore{
        BucketStore{
            Duration: bDuration,
        },
        session,
        math.Pow10(1),
    }
    entryC := make(chan Entry, 5)
    errorC := make(chan error)
    baseTime := time.Now()
    roundedTime := time.Unix(baseTime.Unix() - (baseTime.Unix() % 60), 0)
    spacing, _ := time.ParseDuration("2s")
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

    go store.Query(entryC, seriesUUID, 0, "raw", roundedTime, roundedTime.Add(60 * time.Second), errorC)
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
        case err = <-errorC:
            if err != nil {
                t.Fatal(err)
            }
        }
    }
}
