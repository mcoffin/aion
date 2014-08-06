package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"math"
	"testing"
	"time"
)

func TestMemoryCacheDoubleQuery(t *testing.T) {
	store := MemoryBucketBuilder{
		Duration:   60 * time.Second,
		Multiplier: math.Pow10(1),
	}
	store.Init()
	filter := AggregationFilter{
		Granularity:  0,
		Aggregations: []string{"raw"},
	}
	filter.Init()
	level := Level{
		Filter: &filter,
		Store:  &store,
	}
	level.Filter.SetHandler(level.Store.Insert)
	series := uuid.NewRandom()
	entryTime := time.Now()
	e := Entry{
		Timestamp:  entryTime,
		Attributes: map[string]float64{"raw": 79.1},
	}
	err := level.Filter.Insert(series, e)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 2; i++ {
		entryC := make(chan Entry)
		errorC := make(chan error)
		go func() {
			defer close(entryC)
			level.Store.Query(series, entryTime, entryTime, []string{"raw"}, entryC, errorC)
		}()
		var queryCount int
	loop:
		for queryCount = 0; true; queryCount++ {
			select {
			case err := <-errorC:
				t.Error(err)
				queryCount--
			case q, more := <-entryC:
				if !more {
					break loop
				}
				if q.Attributes["raw"] != e.Attributes["raw"] {
					t.Errorf("Value %v at doesn't match %v\n", q.Attributes["raw"], e.Attributes["raw"])
				}
			}
		}
		if queryCount != 1 {
			t.Errorf("Test %d yielded %d results instead of 1\n", i, queryCount)
		}
	}
}

func TestMemoryCache(t *testing.T) {
	store := MemoryBucketBuilder{
		Duration:   60 * time.Second,
		Multiplier: math.Pow10(1),
	}
	store.Init()
	filter := AggregationFilter{
		Granularity:  0,
		Aggregations: []string{"raw"},
	}
	filter.Init()
	level := Level{
		Filter: &filter,
		Store:  &store,
	}
	testLevel(&level, t, time.Second, store.Duration)
}
