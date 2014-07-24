package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"time"
)

// One entry in a time series
type Entry struct {
	Timestamp  time.Time
	Attributes map[string]float64
}

type Filter interface {
	Insert(series uuid.UUID, entry Entry)
	SetHandler(handler func(uuid.UUID, Entry))
}

type SeriesStore interface {
}

type Level struct {
	Filter Filter
	Store SeriesStore
}

type TimeDB struct {
	Levels []Level
}

func (self *TimeDB) createHandlers() {
	for i := 0; i < len(self.Levels) - 1; i++ {
		nextFilter := self.Levels[i+1].Filter
		self.Levels[i].Filter.SetHandler(nextFilter.Insert)
	}
}

func (self *TimeDB) Put(series uuid.UUID, entry Entry) error {
	self.Levels[0].Filter.Insert(series, entry)
	return nil
}
