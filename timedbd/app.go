package main

import (
	"code.google.com/p/go-uuid/uuid"
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
	"strconv"
	"time"
)

const (
	DefaultPort = 8080
)

type Context struct {
	db *timedb.TimeDB
}

type inputPoint struct {
	Timestamp  int64              `json:"timestamp"`
	Attributes map[string]float64 `json:"attributes"`
}

func (self Context) InsertPoint(res http.ResponseWriter, req *http.Request) {
	seriesUUID := uuid.Parse(mux.Vars(req)["id"])
	dec := json.NewDecoder(req.Body)
	var input inputPoint
	err := dec.Decode(&input)
	if err != nil {
		writeError(res, http.StatusBadRequest, err)
		return
	}
	e := timedb.Entry{
		Timestamp:  time.Unix(input.Timestamp, 0),
		Attributes: input.Attributes,
	}
	fmt.Printf("Inserting (%s, %+v)\n", seriesUUID.String(), e)
	err = self.db.Put(seriesUUID, e)
	if err != nil {
		writeError(res, http.StatusServiceUnavailable, err)
		return
	}
	res.WriteHeader(http.StatusOK)
}

func (self Context) QuerySeries(res http.ResponseWriter, req *http.Request) {
	seriesUUID := uuid.Parse(mux.Vars(req)["id"])
	params := req.URL.Query()
	fmt.Println(params)
	if params["s"] == nil {
		writeError(res, http.StatusBadRequest, errors.New("timedb: no start time given"))
		return
	}
	start, err := parseUnixTime(params["s"][0])
	if err != nil {
		writeError(res, http.StatusBadRequest, err)
		return
	}
	if params["e"] == nil {
		writeError(res, http.StatusBadRequest, errors.New("timedb: no end time given"))
		return
	}
	end, err := parseUnixTime(params["e"][0])
	if err != nil {
		writeError(res, http.StatusBadRequest, err)
		return
	}
	var level int64 = 0
	if params["l"] != nil {
		level, err = strconv.ParseInt(params["l"][0], 10, 64)
		if err != nil {
			writeError(res, http.StatusBadRequest, err)
			return
		}
	}
	entryC := make(chan timedb.Entry)
	errorC := make(chan error)
	go func() {
		defer close(entryC)
		self.db.Levels[level].Store.Query(seriesUUID, start, end, params["a"], entryC, errorC)
	}()
loop:
	for {
		select {
		case err = <-errorC:
			writeError(res, http.StatusServiceUnavailable, err)
		case e, more := <-entryC:
			if !more {
				break loop
			}
			res.Write(mustMarshal(e))
		}
	}
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
	db, err := tempCreateTimeDB()
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
