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

type Level interface {
	Insert(series uuid.UUID, entry Entry) error
	SetCascadeCallback(cb func(uuid.UUID, Entry) error)
}

type TimeDB struct {
	Levels []Level
}

func (self *TimeDB) createCallbacks() {
	for i := 0; i < len(self.Levels)-1; i++ {
		self.Levels[i].SetCascadeCallback(self.Levels[i+1].Insert)
	}
}

func (self *TimeDB) Put(series uuid.UUID, entry Entry) error {
	return self.Levels[0].Insert(series, entry)
}
