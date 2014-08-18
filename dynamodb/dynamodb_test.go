package dynamodb_test

import (
	"math"
	"testing"
	"time"

	"github.com/FlukeNetworks/aion"
	"github.com/FlukeNetworks/aion/aiontest"
	aiondynamodb "github.com/FlukeNetworks/aion/dynamodb"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
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
			Type: dynamodb.TYPE_STRING,
		},
		RangeAttribute: &dynamodb.Attribute{
			Name: "time",
			Type: dynamodb.TYPE_NUMBER,
		},
	}
	tbl := dynamodb.Table{
		Server: server,
		Name:   "timedb",
		Key:    pk,
	}
	cache := aiondynamodb.Cache{
		Table: &tbl,
	}
	level := aion.Level{
		Filter: aion.NewAggregateFilter(0, []string{"raw"}, nil),
		Store:  cache,
	}
	aiontest.TestLevel(&level, t, time.Second, 60*time.Second)
}

func TestDynamoDBStore(t *testing.T) {
	server, err := createDynamoDBTestServer()
	if err != nil {
		t.Fatal(err)
	}
	pk := dynamodb.PrimaryKey{
		KeyAttribute: &dynamodb.Attribute{
			Name: "series",
			Type: dynamodb.TYPE_STRING,
		},
		RangeAttribute: &dynamodb.Attribute{
			Name: "time",
			Type: dynamodb.TYPE_NUMBER,
		},
	}
	tbl := dynamodb.Table{
		Server: server,
		Name:   "timedb-bucket",
		Key:    pk,
	}
	store := aion.NewBucketStore(60*time.Second, math.Pow10(1))
	repo := aiondynamodb.Repository{
		Table: &tbl,
	}
	store.Repository = repo
	level := aion.Level{
		Filter: aion.NewAggregateFilter(0, []string{"raw"}, nil),
		Store:  store,
	}
	aiontest.TestLevel(&level, t, time.Second, store.Duration)
}
