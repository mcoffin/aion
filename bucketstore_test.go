package aion

import (
	"math"
	"testing"
	"time"

	"code.google.com/p/go-uuid/uuid"
)

type fakeQueryFunc func(start, end time.Time, entries chan Entry)

func (self fakeQueryFunc) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	self(start, end, entries)
}

func TestBucketStoreMultiQuery(t *testing.T) {
	store := NewBucketStore(60*time.Second, math.Pow10(1))
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store:  store,
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
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		queryCount := 0
		err = ForAllQuery(series, entryTime, entryTime, []string{"raw"}, level.Store, func(entry Entry) {
			queryCount++
		})
		if err != nil {
			t.Fatal(err)
		}
		if queryCount != 1 {
			t.Errorf("Test %d yielded %d results instead of 1\n", i, queryCount)
		}
	}
}

func TestBucketStoreSourcing(t *testing.T) {
	source := fakeQueryFunc(func(start, end time.Time, entries chan Entry) {
		e := Entry{
			Timestamp:  start,
			Attributes: map[string]float64{"raw": 66.6},
		}
		entries <- e
	})
	store := NewBucketStore(60*time.Second, math.Pow10(1))
	store.Source = source
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	store.Filter = filter
	level := Level{
		Filter: filter,
		Store:  store,
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
		t.Fatal(err)
	}
	bucketTime := entryTime.Truncate(store.Duration)
	queryCount := 0
	err = ForAllQuery(series, bucketTime, bucketTime.Add(store.Duration), []string{"raw"}, level.Store, func(entry Entry) {
		queryCount++
	})
	if err != nil {
		t.Error(err)
	}
	if queryCount != 2 {
		t.Errorf("Queried out %d items instead of 2!\n", queryCount)
	}
}

func TestBucketStore(t *testing.T) {
	store := NewBucketStore(60*time.Second, math.Pow10(1))
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store:  store,
	}
	testLevel(&level, t, time.Second, store.Duration)
}
