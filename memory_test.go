package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"math"
	"testing"
	"time"
)

type fakeQueryFunc func(start, end time.Time, entries chan Entry)

func (self fakeQueryFunc) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	self(start, end, entries)
}

func TestMemoryCacheSourcing(t *testing.T) {
	source := fakeQueryFunc(func(start, end time.Time, entries chan Entry) {
		e := Entry{
			Timestamp:  start,
			Attributes: map[string]float64{"raw": 66.6},
		}
		entries <- e
	})
	store := MemoryBucketBuilder{
		Duration:   60 * time.Second,
		Multiplier: math.Pow10(1),
		Source:     source,
	}
	store.Init()
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store:  &store,
	}
	level.Filter.SetHandler(level.Store.Insert)
	entryTime := time.Now()
	ent := Entry{
		Timestamp:  entryTime,
		Attributes: map[string]float64{"raw": 79.1},
	}
	series := uuid.NewRandom()
	err := level.Filter.Insert(series, ent)
	if err != nil {
		t.Error(err)
	}
	bucketTime := entryTime.Truncate(store.Duration)
	queryCount := 0
	ForAllQuery(series, bucketTime, bucketTime.Add(store.Duration), []string{"raw"}, level.Store, func(entry Entry) {
		queryCount++
	})
	if queryCount != 2 {
		t.Errorf("Queried out %d items instead of 2\n", queryCount)
	}
}

func TestMemoryCacheDoubleQuery(t *testing.T) {
	store := MemoryBucketBuilder{
		Duration:   60 * time.Second,
		Multiplier: math.Pow10(1),
	}
	store.Init()
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
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
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store:  &store,
	}
	testLevel(&level, t, time.Second, store.Duration)
}
