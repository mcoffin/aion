package main

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"github.com/FlukeNetworks/timedb"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"github.com/go-martini/martini"
	"log"
	"math"
	"net/http"
	"time"
)

type TimeDB interface {
	Put(series uuid.UUID, entry timedb.Entry) error
}

func createTimeDB() (*timedb.TimeDB, error) {
	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
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
	cache := timedb.DynamoDBCache{
		Table: &tbl,
	}
	filter := timedb.AggregationFilter{
		Granularity:  0,
		Aggregations: []string{"raw"},
	}
	filter.Init()
	level := timedb.Level{
		Filter: &filter,
		Store:  &cache,
	}

	tbl2 := dynamodb.Table{
		Server: &server,
		Name:   "timedb-bucket",
		Key:    pk,
	}
	builder := &timedb.MemoryBucketBuilder{
		Duration:   60 * time.Second,
		Multiplier: math.Pow10(1),
	}
	builder.Init()
	bs := timedb.BucketStore{
		Granularity: 0,
		Builder:     builder,
	}
	store := timedb.NewDynamoDBStore(bs, &tbl2, builder.Multiplier)
	filter2 := timedb.AggregationFilter{
		Granularity:  0,
		Aggregations: []string{"raw"},
	}
	filter2.Init()
	level2 := timedb.Level{
		Filter: &filter2,
		Store:  store,
	}

	db := timedb.New([]timedb.Level{level, level2})
	return db, nil
}

func errorJson(err error) string {
	return fmt.Sprintf("{ \"message\": \"%s\" }", err.Error())
}

func main() {
	db, err := createTimeDB()
	if err != nil {
		log.Fatal(err)
	}

	m := martini.New()

	m.Use(martini.Recovery())
	m.Use(martini.Logger())
	m.Use(SetContentType)

	r := martini.NewRouter()
	r.Put("/series/:id", InsertDatapoint)

	m.MapTo(db, (*TimeDB)(nil))

	m.Action(r.Handle)

	err = http.ListenAndServe(":7684", m)
	if err != nil {
		log.Fatal(err)
	}
}

type inputPoint struct {
	timestamp  int64
	attributes map[string]float64
}

func InsertDatapoint(tdb TimeDB, req *http.Request, params martini.Params) (int, string) {
	seriesUUID := uuid.Parse(params["id"])
	dec := json.NewDecoder(req.Body)
	var input inputPoint
	err := dec.Decode(&input)
	e := timedb.Entry{
		Timestamp:  time.Unix(input.timestamp, 0),
		Attributes: input.attributes,
	}
	if err != nil {
		return http.StatusBadRequest, errorJson(err)
	}
	err = tdb.Put(seriesUUID, e)
	if err != nil {
		return http.StatusServiceUnavailable, errorJson(err)
	}
	return http.StatusOK, ""
}

func SetContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
