package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"code.google.com/p/go-uuid/uuid"

	"github.com/FlukeNetworks/aion/tagstore"
	influxdb "github.com/influxdb/influxdb/client"
)

type InputPoint struct {
	Timestamp  int64            `json:"timestamp"`
	Attributes map[string]int64 `json:"attributes"`
}

type Context struct {
	Influx             *influxdb.Client
	TagStore           tagstore.TagStore
	StoredAggregations []string
	RollupPeriods      []string
}

type createSeriesReq struct {
	Tags    map[string]string `json:"tags"`
	Rollups []string          `json:"rollups"`
}

func seriesName(series uuid.UUID) string {
	return strings.Replace(series.String(), "-", "", -1)
}

func rollupName(aggregation, field string) string {
	return (field + aggregation)
}

func (self Context) constructRollupQuery(series uuid.UUID, rollups []string, period string) string {
	seriesStr := seriesName(series)
	queryBuf := bytes.NewBufferString("select")
	for _, rollup := range rollups {
		for aIndex, a := range self.StoredAggregations {
			fmt.Fprintf(queryBuf, " %s(%s) as %s", a, rollup, rollupName(a, rollup))
			if aIndex < len(self.StoredAggregations)-1 {
				fmt.Fprint(queryBuf, ",")
			}
		}
	}
	fmt.Fprintf(queryBuf, " from %s group by time(%s) into %s", seriesStr, period, seriesStr+period)
	return queryBuf.String()
}

func (self Context) CreateSeries(res http.ResponseWriter, req *http.Request) {
	series := uuid.NewRandom()
	out := map[string]string{"id": series.String()}
	outData, err := json.Marshal(out)
	if err != nil {
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}
	dec := json.NewDecoder(req.Body)
	defer req.Body.Close()

	var config createSeriesReq
	err = dec.Decode(&config)
	if err != nil {
		http.Error(res, err.Error(), http.StatusBadRequest)
		return
	}

	tags := make([]tagstore.Tag, 0, len(config.Tags))
	for k, v := range config.Tags {
		tags = append(tags, tagstore.Tag{k, v})
	}

	// Store the tag metadata for the series
	err = self.TagStore.Tag(series, tags)
	if err != nil {
		http.Error(res, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// Run the continuous queries for the series on influx
	for _, period := range self.RollupPeriods {
		q := self.constructRollupQuery(series, config.Rollups, period)
		_, err = self.Influx.Query(q, influxdb.Microsecond)
		if err != nil {
			http.Error(res, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	res.Header().Set("Content-Type", "application/json")
	res.Write(outData)
}

func tagsForMap(m map[string][]string) []tagstore.Tag {
	tags := make([]tagstore.Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, tagstore.Tag{k, v[0]})
	}
	return tags
}

func (self Context) SeriesQuery(res http.ResponseWriter, req *http.Request) {
	tags := tagsForMap(req.URL.Query())
	// TODO: should probably catch this error somehow
	series, _ := self.TagStore.Find(tags)
	res.Header().Set("Content-Type", "application/json")
	fmt.Fprint(res, "[")
	first := true
	for s := range series {
		if first {
			first = false
		} else {
			fmt.Fprint(res, ",")
		}
		fmt.Fprintf(res, "\"%s\"", s.String())
	}
	fmt.Fprint(res, "]")
}

func (self Context) DatapointsQuery(res http.ResponseWriter, req *http.Request) {
	// TODO: Query datapoints
}
