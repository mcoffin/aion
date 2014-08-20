package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion"
	"github.com/gorilla/mux"
)

type Context struct {
	db       *aion.Aion
	Endpoint string
}

type createSeriesConfig struct {
	Series uuid.UUID         `json:"-"`
	Tags   map[string]string `json:"tags"`
}

type createSeriesResponse struct {
	Series string `json:"id"`
}

func (self Context) findSeries(params map[string][]string) ([]uuid.UUID, error) {
	tags := make([]aion.Tag, len(params))
	i := 0
	for k, v := range params {
		tags[i] = aion.Tag{
			Name:  k,
			Value: v[0],
		}
		i++
	}
	return self.db.TagStore.Find(tags)
}

func (self Context) Query(res http.ResponseWriter, req *http.Request) {
	params := req.URL.Query()
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
	attributes := params["a"]
	// Delete the query parameters, leaving only the tags
	delete(params, "s")
	delete(params, "e")
	delete(params, "l")
	delete(params, "a")
	seriesList, err := self.findSeries(params)
	if err != nil {
		writeError(res, http.StatusServiceUnavailable, err)
		return
	}
	responses := make(map[string][]byte, len(seriesList))
	urlBuf := bytes.NewBufferString(self.Endpoint + "/v1/series/%s?")
	fmt.Fprintf(urlBuf, "s=%d&e=%d&l=%d", start.Unix(), end.Unix(), level)
	for _, attrib := range attributes {
		fmt.Fprint(urlBuf, "&a="+attrib)
	}
	var wg sync.WaitGroup
	var mutex sync.Mutex
	wg.Add(len(seriesList))
	for _, series := range seriesList {
		// Spawn a goroutine to run each of the required gets
		go func() {
			// This has to be done because we have an early return error code path
			defer wg.Done()
			resp, err := http.Get(fmt.Sprintf(urlBuf.String(), series.String()))
			if err != nil {
				writeError(res, http.StatusServiceUnavailable, err)
				return
			}
			defer resp.Body.Close()
			// TODO: fix this high memory usage
			mutex.Lock()
			responses[series.String()], err = ioutil.ReadAll(resp.Body)
			mutex.Unlock()
		}()
	}
	wg.Wait()
	fmt.Fprint(res, "{")
	i := 0
	for seriesStr, data := range responses {
		seriesStrData, _ := json.Marshal(seriesStr)
		res.Write(seriesStrData)
		fmt.Fprint(res, ":")
		res.Write(data)
		if i < len(responses)-1 {
			fmt.Fprint(res, ",")
		}
		i++
	}
	fmt.Fprint(res, "}")
}

func (self Context) TagQuery(res http.ResponseWriter, req *http.Request) {
	seriesList, err := self.findSeries(req.URL.Query())
	if err != nil {
		writeError(res, http.StatusServiceUnavailable, err)
	}
	fmt.Fprint(res, "[")
	for i, series := range seriesList {
		fmt.Fprintf(res, "\"%s\"", series.String())
		if i < len(seriesList)-1 {
			fmt.Fprint(res, ",")
		}
	}
	fmt.Fprint(res, "]")
}

func (self Context) CreateSeries(res http.ResponseWriter, req *http.Request) {
	dec := json.NewDecoder(req.Body)
	var config createSeriesConfig
	err := dec.Decode(&config)
	if err != nil {
		writeError(res, http.StatusBadRequest, err)
		return
	}
	config.Series = uuid.NewRandom()
	tags := make([]aion.Tag, len(config.Tags))
	i := 0
	for t, v := range config.Tags {
		tags[i] = aion.Tag{
			Name:  t,
			Value: v,
		}
		i++
	}
	err = self.db.TagStore.Tag(config.Series, tags)
	if err != nil {
		writeError(res, http.StatusServiceUnavailable, err)
		return
	}
	resStruct := createSeriesResponse{config.Series.String()}
	data, _ := json.Marshal(resStruct)
	res.Write(data)
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
	e := aion.Entry{
		Timestamp:  time.Unix(input.Timestamp, 0),
		Attributes: input.Attributes,
	}
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
	entryC := make(chan aion.Entry)
	errorC := make(chan error)
	go func() {
		defer close(entryC)
		self.db.Levels[level].Store.Query(seriesUUID, start, end, params["a"], entryC, errorC)
	}()
	fmt.Fprint(res, "[")
	isFirst := true
loop:
	for {
		select {
		case err = <-errorC:
			writeError(res, http.StatusServiceUnavailable, err)
		case e, more := <-entryC:
			if !more {
				break loop
			}
			if !isFirst {
				fmt.Fprint(res, ",")
			} else {
				isFirst = false
			}
			res.Write(mustMarshal(e))
		}
	}
	fmt.Fprint(res, "]")
}
