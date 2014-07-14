package timedb

import (
    "testing"
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
    q, err := cache.Querier(0, "raw")
    if err != nil {
        t.Fatal(err)
    }
    go q.Query(entryC, seriesUUID, start, end, errorC)
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
