package timedb

import (
    "time"
    "code.google.com/p/go-uuid/uuid"
)

type Entry struct {
    Timestamp time.Time
    Value float64
}

type QueryLevel interface {
    Insert(entries chan Entry, series uuid.UUID, success chan error)
}

type TimeDB struct {
    QueryLevels []QueryLevel
}

type InputPoint struct {
    Series uuid.UUID
    Value float64
}

func NewTimeDB(qLevels ...QueryLevel) *TimeDB {
    db := &TimeDB{
        QueryLevels: qLevels,
    }
    return db
}

func (self *TimeDB) Put(point InputPoint, t time.Time) error {
    entryC := make(chan Entry) // No buffer because we're only sending one value
    errorC := make(chan error)
    go self.QueryLevels[0].Insert(entryC, point.Series, errorC)
    entryC <- Entry{
        Timestamp: time.Now(),
        Value: point.Value,
    }
    return <-errorC
}
