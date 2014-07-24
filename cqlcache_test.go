package timedb

import (
	"github.com/gocql/gocql"
	"testing"
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
	filter := AggregationFilter{
		Granularity:  0,
		Aggregations: []string{"raw"},
	}
	filter.Init()
	level := Level{
		Filter: &filter,
		Store:  &cache,
	}
	testLevel(&level, t)
}
