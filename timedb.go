package timedb

import (
    "time"
    "code.google.com/p/go-uuid/uuid"
)

const (
    channelSize = 4
)

// One entry in a time series
type Entry struct {
    Timestamp time.Time
    Value float64
}

type SeriesStore interface {
    Insert(series uuid.UUID, entry Entry, aggregator string) error
}

type TimeDB struct {
    Stores []SeriesStore
}

func NewTimeDB(qLevels ...QueryLevel) *TimeDB {
    db := &TimeDB{
        QueryLevels: qLevels,
    }
    return db
}
