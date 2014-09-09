package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"code.google.com/p/go-uuid/uuid"

	"github.com/FlukeNetworks/aion/meta"
	influxdb "github.com/influxdb/influxdb/client"
)

type Context struct {
	Influx             *influxdb.Client
	InfluxConfig       *influxdb.ClientConfig
	MetaStore          meta.Store
	MetaSearcher       meta.Searcher
	StoredAggregations []string
	RollupPeriods      []string
}

type createSeriesReq struct {
	Metadata interface{} `json:"metadata"`
	Rollups  []string    `json:"rollups"`
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

	// Put the series in the MetaStore
	err = self.MetaStore.Index(series, config)
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
	} else {
		where = []string{"where " + where[0]}
	}
	delete(params, "where")

	series, err := self.MetaSearcher.Search(params["q"][0])
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
