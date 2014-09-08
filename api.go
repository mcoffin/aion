package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"code.google.com/p/go-uuid/uuid"

	"github.com/FlukeNetworks/aion/tags"
	"github.com/gorilla/mux"
	influxdb "github.com/influxdb/influxdb/client"
)

type InputPoint struct {
	Timestamp  int64            `json:"timestamp"`
	Attributes map[string]int64 `json:"attributes"`
}

type Context struct {
	Influx             *influxdb.Client
	InfluxConfig       *influxdb.ClientConfig
	TagStore           tags.Store
	TagSearcher        tags.Searcher
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

	ts := make([]tags.Tag, 0, len(config.Tags))
	for k, v := range config.Tags {
		ts = append(ts, tags.Tag{k, v})
	}

	// Store the tag metadata for the series
	err = self.TagStore.Insert(series, ts)
	if err != nil {
		http.Error(res, err.Error(), http.StatusServiceUnavailable)
		return
	}
	err = self.TagSearcher.Insert(series, ts)
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

func tagsForMap(m map[string][]string) []tags.Tag {
	ts := make([]tags.Tag, 0, len(m))
	for k, v := range m {
		ts = append(ts, tags.Tag{k, v[0]})
	}
	return ts
}

func (self Context) SeriesQuery(res http.ResponseWriter, req *http.Request) {
	tags := tagsForMap(req.URL.Query())
	series, err := self.TagSearcher.Find(tags)
	if err != nil {
		http.Error(res, err.Error(), http.StatusServiceUnavailable)
		return
	}
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

func (self Context) TagQuery(res http.ResponseWriter, req *http.Request) {
	seriesUUID := uuid.Parse(mux.Vars(req)["id"])
	ts, err := self.TagStore.Query(seriesUUID)
	if err != nil {
		http.Error(res, err.Error(), http.StatusServiceUnavailable)
		return
	}
	res.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(res)
	enc.Encode(ts)
}

func (self Context) DatapointsQuery(res http.ResponseWriter, req *http.Request) {
	params := req.URL.Query()

	if params["select"] == nil {
		http.Error(res, "aion: No selection made", http.StatusBadRequest)
		return
	}
	selectClause := params["select"]
	delete(params, "select")
	period := params["period"]
	if period == nil {
		period = []string{""}
	}
	delete(params, "period")
	where := params["where"]
	if where == nil {
		where = []string{""}
	}
	delete(params, "where")

	tags := tagsForMap(params)
	series, err := self.TagSearcher.Find(tags)
	if err != nil {
		http.Error(res, err.Error(), http.StatusServiceUnavailable)
		return
	}
	seriesToQuery := []string{}
	for s := range series {
		seriesToQuery = append(seriesToQuery, seriesName(s)+period[0])
	}
	q := fmt.Sprintf("select %s from %s %s", selectClause[0], strings.Join(seriesToQuery, " merge "), where[0])
	newUrl := fmt.Sprintf("http://%s/db/%s/series?q=%s&u=%s&p=%s", self.InfluxConfig.Host, self.InfluxConfig.Database, url.QueryEscape(q), self.InfluxConfig.Username, self.InfluxConfig.Password)
	http.Redirect(res, req, newUrl, http.StatusSeeOther)
}
