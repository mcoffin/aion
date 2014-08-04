package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"time"
)

type EntryReader interface {
	ReadEntries(entries []Entry) (int, error)
}

type entryReaderFunc (func([]Entry) (int, error))

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
	Flush(series uuid.UUID) error
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

// A level represents one granularity of data storage in timedb
type Level struct {
	Filter Filter
	Store  SeriesStore
}

// Root of the TimeDB API
type TimeDB struct {
	Levels []Level
}

func New(levels []Level) *TimeDB {
	ret := &TimeDB{
		Levels: levels,
	}
	ret.createHandlers()
	return ret
}

func (self *TimeDB) createHandlers() {
	for i := 0; i < len(self.Levels)-1; i++ {
		thisLevel := self.Levels[i]
		nextLevel := self.Levels[i+1]
		thisLevel.Filter.SetHandler(func(series uuid.UUID, entry Entry) error {
			err := thisLevel.Store.Insert(series, entry)
			if err != nil {
				return err
			}
			err = nextLevel.Filter.Insert(series, entry)
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
func (self *TimeDB) Put(series uuid.UUID, entry Entry) error {
	return self.Levels[0].Filter.Insert(series, entry)
}
