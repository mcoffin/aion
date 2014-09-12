package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/BurntSushi/toml"
	"github.com/FlukeNetworks/aion/elastisearch"
	"github.com/codegangsta/negroni"
	"github.com/mattbaird/elastigo/lib"

	"github.com/gorilla/mux"
	influxdb "github.com/influxdb/influxdb/client"
)

const (
	DefaultHttp = ":8081"
)

type rollupConfig struct {
	Period            string `toml:"period"`
	BucketDuration    string `toml:"bucket-duration"`
	RetentionPolicy   string `toml:"retention-policy"`
	ShardDuration     string `toml:"shard-duration"`
	ReplicationFactor uint32 `toml:"replication-factor"`
}

func (self rollupConfig) Regex() string {
	return "/.*" + self.Period + "/"
}

type config struct {
	StoredAggregations []string       `toml:"stored-aggregations"`
	Rollups            []rollupConfig `toml:"rollup"`
}

func ensureDatabase(client *influxdb.Client, clientConfig *influxdb.ClientConfig, database string, cfg config) error {
	// First ensure that the aion database exists
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
		spaces := make([]interface{}, 0, len(cfg.Rollups)+1)
		spaces = append(spaces, map[string]interface{}{
			"name":              "raw",
			"regex":             "/[0-9a-f]{32}/",
			"retentionPolicy":   "inf", // TODO: real retention policy
			"shardDuration":     "1d",
			"replicationFactor": 1, // TODO: real replication factor
			"split":             1,
			"schema-config": map[string]interface{}{
				"duration": "1m", // TODO: real duration
			},
		})
		// Since we had to create the database, we also have to create the shard spaces
		for _, rollup := range cfg.Rollups {
			spaceDesc := map[string]interface{}{
				"name":              rollup.Period,
				"regex":             rollup.Regex(),
				"retentionPolicy":   rollup.RetentionPolicy,
				"shardDuration":     rollup.ShardDuration,
				"replicationFactor": rollup.ReplicationFactor,
				"split":             1,
				"schema-config": map[string]interface{}{
					"duration": rollup.BucketDuration,
				},
			}
			spaces = append(spaces, spaceDesc)
		}
		reqJson := map[string]interface{}{"spaces": spaces}
		data, _ := json.Marshal(reqJson)
		url := fmt.Sprintf("http://%s/cluster/database_configs/%s?u=%s&p=%s", clientConfig.Host, clientConfig.Database, url.QueryEscape(clientConfig.Username), url.QueryEscape(clientConfig.Password))
		log.Printf("Calling db configure with:\n%s\n", string(data))
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("Error creating database: influxdb returned %s", resp.Status)
		}
	}
	return nil
}

func main() {
	bind := flag.String("http", DefaultHttp, "Http bind address")
	configFile := flag.String("config", "config.toml", "Config file")

	influxHost := flag.String("influx-host", "localhost:8086", "InfluxDB host")
	influxUser := flag.String("influx-user", "root", "InfluxDB username")
	influxPass := flag.String("influx-pass", "root", "InfluxDB password")
	influxDatabase := flag.String("influx-db", "aion", "InfluxDB database")

	elastisearchHost := flag.String("elastisearch-host", "localhost", "elastisearch host")
	elastisearchIndex := flag.String("elastisearch-index", "aion", "elasticsearch index name")

	flag.Parse()

	var cfg config
	_, err := toml.DecodeFile(*configFile, &cfg)
	if err != nil {
		log.Fatal(err)
	}

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

	err = ensureDatabase(influxClient, &influxConfig, influxConfig.Database, cfg)
	if err != nil {
		log.Fatal(err)
	}

	metastore := &elastisearch.Metastore{Connection: elastigo.NewConn(), IndexName: *elastisearchIndex}
	metastore.Connection.Domain = *elastisearchHost

	log.Printf("Using config: %+v\n", cfg)

	rollupPeriods := make([]string, 0, len(cfg.Rollups))
	for _, rollup := range cfg.Rollups {
		rollupPeriods = append(rollupPeriods, rollup.Period)
	}

	ctx := Context{
		Influx:             influxClient,
		InfluxConfig:       &influxConfig,
		MetaStore:          metastore,
		MetaSearcher:       metastore,
		StoredAggregations: cfg.StoredAggregations,
		RollupPeriods:      rollupPeriods,
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
