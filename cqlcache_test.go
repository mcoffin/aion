package timedb

import (
    "testing"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
)

const (
    clusterIp = "172.28.128.2"
    keyspace = "timedb"
)

func TestCQLCache(t *testing.T) {
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
