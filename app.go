package main

import (
	"flag"
	"log"
	"net/http"

	aiondynamodb "github.com/FlukeNetworks/aion/dynamodb"
	"github.com/FlukeNetworks/aion/tags"
	"github.com/codegangsta/negroni"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"github.com/gorilla/mux"
	influxdb "github.com/influxdb/influxdb/client"
)

const (
	DefaultHttp = ":8081"
)

func ensureDatabase(client *influxdb.Client, database string) error {
	databases, err := client.GetDatabaseList()
	if err != nil {
		return err
	}
	found := false
	for _, dbMap := range databases {
		if dbMap["name"] == database {
			found = true
			break
		}
	}
	if !found {
		return client.CreateDatabase(database)
	}
	return nil
}

func seedSearcher(src tags.Store, searcher tags.Searcher) error {
	series, err := src.Scan()
	if err != nil {
		return err
	}
	for s := range series {
		searcher.Insert(s.SeriesID, s.Tags)
	}
	return nil
}

func main() {
	bind := flag.String("http", DefaultHttp, "Http bind address")
	influxHost := flag.String("influx-host", "localhost:8086", "InfluxDB host")
	influxUser := flag.String("influx-user", "root", "InfluxDB username")
	influxPass := flag.String("influx-pass", "root", "InfluxDB password")
	influxDatabase := flag.String("influx-db", "aion", "InfluxDB database")
	flag.Parse()

	influxConfig := influxdb.ClientConfig{
		Host:       *influxHost,
		Username:   *influxUser,
		Password:   *influxPass,
		Database:   *influxDatabase,
		HttpClient: http.DefaultClient,
	}
	influxClient, err := influxdb.New(&influxConfig)
	if err != nil {
		log.Fatal(err)
	}

	err = ensureDatabase(influxClient, influxConfig.Database)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: load from env
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}
	server := &dynamodb.Server{
		Auth:   auth,
		Region: aws.Region{Name: "localhost", DynamoDBEndpoint: "http://localhost:8000"},
	}
	table := dynamodb.Table{
		Server: server,
		Name:   "aion",
		Key: dynamodb.PrimaryKey{
			KeyAttribute: &dynamodb.Attribute{
				Type: dynamodb.TYPE_STRING,
				Name: "series",
			},
			RangeAttribute: nil,
		},
	}

	ctx := Context{
		Influx:       influxClient,
		InfluxConfig: &influxConfig,
		// TODO: load this from environment
		TagStore:           &aiondynamodb.TagStore{table},
		TagSearcher:        nil,
		StoredAggregations: []string{"min", "max", "mean", "count"},
		RollupPeriods:      []string{"1m"},
	}

	err = seedSearcher(ctx.TagStore, ctx.TagSearcher)
	if err != nil {
		log.Fatal(err)
	}

	router := mux.NewRouter()
	rv1 := router.PathPrefix("/v1").Subrouter()
	rv1.HandleFunc("/series", ctx.CreateSeries).Methods("POST")
	rv1.HandleFunc("/series", ctx.SeriesQuery).Methods("GET")
	rv1.HandleFunc("/series/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/tags", ctx.TagQuery).Methods("GET")
	rv1.HandleFunc("/datapoints", ctx.DatapointsQuery).Methods("GET")

	n := negroni.Classic()
	n.UseHandler(router)
	http.Handle("/v1/", n)
	http.ListenAndServe(*bind, nil)
}
