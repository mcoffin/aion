package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/FlukeNetworks/aion"
	"github.com/codegangsta/negroni"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"github.com/gorilla/mux"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"
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
	cache := aion.DynamoDBCache{
		Table: &tbl,
	}
	filter := aion.NewAggregateFilter(0, []string{"raw"}, nil)
	level := aion.Level{
		Filter: filter,
		Store:  &cache,
	}

	tbl2 := dynamodb.Table{
		Server: &server,
		Name:   "timedb-bucket",
		Key:    pk,
	}
	builder := &aion.MemoryBucketBuilder{
		Duration:   60 * time.Second,
		Multiplier: math.Pow10(1),
	}
	builder.Init()
	bs := aion.BucketStore{
		Granularity: 0,
		Builder:     builder,
	}
	store := aion.NewDynamoDBStore(bs, &tbl2, builder.Multiplier)
	filter2 := aion.NewAggregateFilter(0, []string{"raw"}, nil)
	level2 := aion.Level{
		Filter: filter2,
		Store:  store,
	}

	db := aion.New([]aion.Level{level, level2})
	return db, nil
}
