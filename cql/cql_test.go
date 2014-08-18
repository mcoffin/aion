package aion

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
)

func newCQLTestSession() (*gocql.Session, error) {
	cluster := gocql.NewCluster("172.28.128.2")
	cluster.Keyspace = "timedb"
	return cluster.CreateSession()
}

func TestCQLCache(t *testing.T) {
	session, err := newCQLTestSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	cache := CQLCache{
		ColumnFamily: "cache",
		Session:      session,
	}
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store:  &cache,
	}
	testLevel(&level, t, time.Second, 60*time.Second)
}
