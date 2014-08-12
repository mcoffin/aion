package aion

import (
	"github.com/gocql/gocql"
	"testing"
	"time"
)

/*
func TestCQLTagStore(t *testing.T) {
	cluster := gocql.NewCluster("172.28.128.2")
	cluster.Keyspace = "timedb"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	store := CQLTagStore{
		ColumnFamily: "tags",
		Session: session,
	}
	testTagStore(store, t)
}
*/

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

func TestCQLStore(t *testing.T) {
	cluster := gocql.NewCluster("172.28.128.2")
	cluster.Keyspace = "timedb"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	builder := &MemoryBucketBuilder{
		Duration: 60 * time.Second,
		Multiplier: 10,
	}
	builder.Init()
	bs := BucketStore{
		Granularity: 0,
		Builder: builder,
	}
	store := NewCQLStore(bs, session, builder.Multiplier, builder.Duration)
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store: store,
	}
	testLevel(&level, t, time.Second, builder.Duration)
}
