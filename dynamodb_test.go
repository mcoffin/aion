package timedb

import (
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"testing"
	"time"
)

func createDynamoDBTestServer() (*dynamodb.Server, error) {
	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
	}
	server := dynamodb.Server{
		Auth:   auth,
		Region: aws.Region{Name: "us-west-1", DynamoDBEndpoint: "http://localhost:8000"},
	}
	return &server, nil
}

func TestDynamoDBCache(t *testing.T) {
	server, err := createDynamoDBTestServer()
	if err != nil {
		t.Fatal(err)
	}
	pk := dynamodb.PrimaryKey{
		KeyAttribute: &dynamodb.Attribute{
			Name: "series",
			Type: "S",
		},
		RangeAttribute: &dynamodb.Attribute{
			Name: "time",
			Type: "N",
		},
	}
	tbl := dynamodb.Table{
		Server: server,
		Name:   "timedb",
		Key:    pk,
	}
	cache := DynamoDBCache{
		Table: &tbl,
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
	testLevel(&level, t, time.Second, 60*time.Second)
}
