package main

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/FlukeNetworks/aion"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"
)

type Context struct {
	db *aion.Aion
}

type createSeriesConfig struct {
	Series uuid.UUID            `json:"-"`
	Tags   map[string]string `json:"tags"`
}

type createSeriesResponse struct {
	Series string `json:"id"`
}

func (self Context) TagQuery(res http.ResponseWriter, req *http.Request) {
	writeError(res, http.StatusNotImplemented, errors.New("query by tags not implemented"))
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
	// TODO: create aion series from config
	tags := make([]aion.Tag, len(config.Tags))
	i := 0
	for t, v := range config.Tags {
		tags[i] = aion.Tag{
			Name: t,
			Value: v,
		}
		i++
	}
	fmt.Println(tags)
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
