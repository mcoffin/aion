package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/FlukeNetworks/aion/elastisearch"
	"github.com/codegangsta/negroni"
	"github.com/mattbaird/elastigo/lib"

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

func main() {
	bind := flag.String("http", DefaultHttp, "Http bind address")

	influxHost := flag.String("influx-host", "localhost:8086", "InfluxDB host")
	influxUser := flag.String("influx-user", "root", "InfluxDB username")
	influxPass := flag.String("influx-pass", "root", "InfluxDB password")
	influxDatabase := flag.String("influx-db", "aion", "InfluxDB database")

	elastisearchHost := flag.String("elastisearch-host", "localhost", "elastisearch host")

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

	metastore := &elastisearch.Metastore{Connection: elastigo.NewConn(), IndexName: "aion"}
	metastore.Connection.Domain = *elastisearchHost

	ctx := Context{
		Influx:       influxClient,
		InfluxConfig: &influxConfig,
		// TODO: load this from environment
		MetaStore:          metastore,
		MetaSearcher:       metastore,
		StoredAggregations: []string{"min", "max", "mean", "count"},
		RollupPeriods:      []string{"1m"},
	}

	router := mux.NewRouter()
	rv1 := router.PathPrefix("/v1").Subrouter()
	rv1.HandleFunc("/series", ctx.CreateSeries).Methods("POST")
	rv1.HandleFunc("/datapoints", ctx.DatapointsQuery).Methods("GET")

	n := negroni.Classic()
	n.UseHandler(router)
	http.Handle("/v1/", n)
	http.ListenAndServe(*bind, nil)
}
