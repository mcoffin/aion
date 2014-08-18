package aion_test

import (
	"math"
	"testing"
	"time"

	"github.com/FlukeNetworks/aion"
	"github.com/FlukeNetworks/aion/aiontest"

	"code.google.com/p/go-uuid/uuid"
)

type fakeQueryFunc func(start, end time.Time, entries chan aion.Entry)

func (self fakeQueryFunc) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan aion.Entry, errors chan error) {
	self(start, end, entries)
}

func TestBucketStoreMultiQuery(t *testing.T) {
	store := aion.NewBucketStore(60*time.Second, math.Pow10(1))
	filter := aion.NewAggregateFilter(0, []string{"raw"}, nil)
	level := aion.Level{
		Filter: filter,
		Store:  store,
	}
	level.Filter.SetHandler(level.Store.Insert)
	entryTime := time.Now()
	ent := aion.Entry{
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
		err = aion.ForAllQuery(series, entryTime, entryTime, []string{"raw"}, level.Store, func(entry aion.Entry) {
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
	source := fakeQueryFunc(func(start, end time.Time, entries chan aion.Entry) {
		e := aion.Entry{
			Timestamp:  start,
			Attributes: map[string]float64{"raw": 66.6},
		}
		entries <- e
	})
	store := aion.NewBucketStore(60*time.Second, math.Pow10(1))
	store.Source = source
	filter := aion.NewAggregateFilter(0, []string{"raw"}, nil)
	store.Filter = filter
	level := aion.Level{
		Filter: filter,
		Store:  store,
	}
	level.Filter.SetHandler(level.Store.Insert)
	entryTime := time.Now()
	ent := aion.Entry{
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
	err = aion.ForAllQuery(series, bucketTime, bucketTime.Add(store.Duration), []string{"raw"}, level.Store, func(entry aion.Entry) {
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
	store := aion.NewBucketStore(60*time.Second, math.Pow10(1))
	filter := aion.NewAggregateFilter(0, []string{"raw"}, nil)
	level := aion.Level{
		Filter: filter,
		Store:  store,
	}
	aiontest.TestLevel(&level, t, time.Second, store.Duration)
}
