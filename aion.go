package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"time"
)

// Generalization for something that can read entries in to a buffer
type EntryReader interface {
	ReadEntries(entries []Entry) (int, error)
}

// Convenience type for using a read function as a full-fledged EntryReader
type entryReaderFunc (func([]Entry) (int, error))

// entryReaderFunc implements the EntryReader interface
func (self entryReaderFunc) ReadEntries(entries []Entry) (int, error) {
	return self(entries)
}

// One entry in a time series
type Entry struct {
	Timestamp  time.Time          `json:"timestamp"`
	Attributes map[string]float64 `json:"attributes"`
}

// Interface of filtering data in to a level
type Filter interface {
	Insert(series uuid.UUID, entry Entry) error
	SetHandler(handler func(uuid.UUID, Entry) error)
}

// Interface for something that can provide time series data back
type Querier interface {
	Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error)
}

// Interface for storing time series data
type SeriesStore interface {
	Querier
	Insert(series uuid.UUID, entry Entry) error
}

// A tag for a time series
type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Interface for storing tag,value,series triples
type TagStore interface {
	Tag(series uuid.UUID, tags []Tag) error
	Find(tags []Tag) ([]uuid.UUID, error)
}

// A level represents one granularity of data storage in timedb
type Level struct {
	Filter Filter
	Store  SeriesStore
}

// Root of the Aion API
type Aion struct {
	TagStore TagStore
	Levels []Level
}

// Creates a new Aion instance with a given set of levels
func New(levels []Level, ts TagStore) *Aion {
	ret := &Aion{
		TagStore: ts,
		Levels: levels,
	}
	ret.createHandlers()
	return ret
}

// "Hooks-up" the levels in an Aion instance. Usually called from New()
func (self *Aion) createHandlers() {
	for i := 0; i < len(self.Levels)-1; i++ {
		thisLevel := self.Levels[i]
		nextLevel := self.Levels[i+1]
		thisLevel.Filter.SetHandler(func(series uuid.UUID, entry Entry) error {
			err := nextLevel.Filter.Insert(series, entry)
			if err != nil {
				return err
			}
			err = thisLevel.Store.Insert(series, entry)
			if err != nil {
				return err
			}
			return nil
		})
	}
	lastLevel := self.Levels[len(self.Levels)-1]
	lastLevel.Filter.SetHandler(lastLevel.Store.Insert)
}

// Convenience method for inserting one data point into the first level
// (possibly triggering rollups
func (self *Aion) Put(series uuid.UUID, entry Entry) error {
	return self.Levels[0].Filter.Insert(series, entry)
}

// Convenience method for querying. Handles concurrency for you, calling `handler` for every entry that is queried out
func ForAllQuery(series uuid.UUID, start, end time.Time, attributes []string, q Querier, handler func(Entry)) error {
	var err error
	entryC := make(chan Entry)
	errorC := make(chan error)
	go func() {
		defer close(entryC)
		q.Query(series, start, end, attributes, entryC, errorC)
	}()
loop:
	for {
		select {
		case err = <-errorC:
		case e, more := <-entryC:
			if !more {
				break loop
			}
			handler(e)
		}
	}
	return err
}
