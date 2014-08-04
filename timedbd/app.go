package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/FlukeNetworks/timedb"
	"github.com/codegangsta/negroni"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"github.com/gorilla/mux"
	"log"
	"math"
	"net/http"
	"time"
)

const (
	DefaultPort = 8080
)

type Context struct {
	db *timedb.TimeDB
}

func (self Context) InsertPoint(res http.ResponseWriter, req *http.Request) {
	res.WriteHeader(http.StatusNotImplemented)
	err := errors.New(http.StatusText(http.StatusNotImplemented))
	res.Write(mustMarshal(Error{err.Error()}))
}

type Error struct {
	Error string `json:"error"`
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func main() {
	// Setup flags
	port := flag.Int("port", DefaultPort, "port on which to listen")
	flag.Parse()

	// Setup the database context
	db, err := tempCreateTimeDB()
	if err != nil {
		log.Fatal(err)
	}
	ctx := Context{db}

	// Setup routes
	r := mux.NewRouter()
	r.HandleFunc("/series/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", ctx.InsertPoint).Methods("POST")

	// Setup basic recovery and logging middleware
	n := negroni.Classic()
	n.Use(negroni.HandlerFunc(func(res http.ResponseWriter, req *http.Request, next http.HandlerFunc) {
		res.Header().Set("Content-Type", "application/json")
		next(res, req)
	}))
	n.UseHandler(r)
	n.Run(fmt.Sprintf(":%d", *port))
}

func tempCreateTimeDB() (*timedb.TimeDB, error) {
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
