package timedb

import (
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"testing"
)

func TestDynamoDBCache(t *testing.T) {
	auth, err := aws.EnvAuth()
	if err != nil {
		t.Fatal(err)
	}
	server := dynamodb.Server{
		Auth:   auth,
		Region: aws.Region{Name: "us-west-1", DynamoDBEndpoint: "http://localhost:8000"},
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
		Server: &server,
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
	testLevel(&level, t)
}
