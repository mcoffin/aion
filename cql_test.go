package aion

import (
	"github.com/gocql/gocql"
	"testing"
	"time"
)

func TestCQLCache(t *testing.T) {
	cluster := gocql.NewCluster("172.28.128.2")
	cluster.Keyspace = "timedb"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	cache := CQLCache{
		Session: session,
	}
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store:  &cache,
	}
	testLevel(&level, t, time.Second, 60*time.Second)
}
