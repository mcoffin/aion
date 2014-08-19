package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/FlukeNetworks/aion"
	"github.com/FlukeNetworks/aion/cayley"
	aiondynamodb "github.com/FlukeNetworks/aion/dynamodb"
	"github.com/codegangsta/negroni"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/leveldb"
	"github.com/gorilla/mux"
)

const (
	DefaultPort = 8080
)

type inputPoint struct {
	Timestamp  int64              `json:"timestamp"`
	Attributes map[string]float64 `json:"attributes"`
}

type Error struct {
	error
}

func (self Error) MarshalJSON() ([]byte, error) {
	e := struct {
		Error string `json:"error"`
	}{
		self.Error(),
	}
	return json.Marshal(e)
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func writeError(res http.ResponseWriter, status int, err error) {
	res.WriteHeader(status)
	res.Write(mustMarshal(Error{err}))
}

func parseUnixTime(s string) (time.Time, error) {
	unix, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(unix, 0), nil
}

func main() {
	// Setup flags
	port := flag.Int("port", DefaultPort, "port on which to listen")
	flag.Parse()

	// Setup the database context
	db, err := tempCreateAion()
	if err != nil {
		log.Fatal(err)
	}
	ctx := Context{db}

	// Setup routes
	router := mux.NewRouter()
	r := router.PathPrefix("/v1").Subrouter()
	r.HandleFunc("/series/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", ctx.InsertPoint).Methods("POST")
	r.HandleFunc("/series/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", ctx.QuerySeries).Methods("GET")
	r.HandleFunc("/series", ctx.TagQuery).Methods("GET")
	r.HandleFunc("/series", ctx.CreateSeries).Methods("POST")
	r.HandleFunc("/datapoints", ctx.Query).Methods("GET")

	// Setup basic recovery and logging middleware
	n := negroni.Classic()
	// Always return JSON, so add header with middleware to avoid code duplication
	n.Use(negroni.HandlerFunc(func(res http.ResponseWriter, req *http.Request, next http.HandlerFunc) {
		res.Header().Set("Content-Type", "application/json")
		next(res, req)
	}))
	n.UseHandler(router)
	http.Handle("/v1/", n)
	http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
}

func tempCreateAion() (*aion.Aion, error) {
	// Create Cayley TagStore
	ts, err := graph.NewTripleStore("leveldb", "/tmp/aion", nil)
	if err != nil {
		return nil, err
	}
	tagStore := cayley.TagStore{
		TripleStore: ts,
	}

	// Create generic dynamodb stuff
	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
	}
	server := &dynamodb.Server{
		Auth:   auth,
		Region: aws.Region{Name: "us-west-1", DynamoDBEndpoint: "http://localhost:8000"},
	}

	// Create level 0 = cache
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
	level0 := aion.Level{
		Filter: aion.NewAggregateFilter(0, []string{"raw"}, nil),
		Store:  cache,
	}

	// Create level1 = bucket store
	tbl2 := dynamodb.Table{
		Server: server,
		Name:   "timedb-bucket",
		Key:    pk,
	}
	store := aion.NewBucketStore(60*time.Second, math.Pow10(1))
	repo := aiondynamodb.Repository{
		Table: &tbl2,
	}
	store.Repository = repo
	level1 := aion.Level{
		Filter: aion.NewAggregateFilter(0, []string{"raw"}, nil),
		Store:  store,
	}

	// Create aion instance
	db := aion.New([]aion.Level{level0, level1}, tagStore)
	return db, nil
}
