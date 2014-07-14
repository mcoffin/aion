package timedb

import (
    "time"
    "code.google.com/p/go-uuid/uuid"
)

type Entry struct {
    Timestamp time.Time
    Value float64
}

type Querier interface {
    Query(entries chan Entry, series uuid.UUID, start time.Time, end time.Time, success chan error)
}

type QueryLevel interface {
    Insert(entries chan Entry, series uuid.UUID, success chan error)
    Querier(granularity time.Duration, aggregator string) (Querier, error)
}

type TimeDB struct {
    QueryLevels []QueryLevel
}

func NewTimeDB(qLevels ...QueryLevel) *TimeDB {
    db := &TimeDB{
        QueryLevels: qLevels,
    }
    return db
}

func (self *TimeDB) Put(series uuid.UUID, value float64, t time.Time) error {
    entryC := make(chan Entry) // No buffer because we're only sending one value
    errorC := make(chan error)
    go self.QueryLevels[0].Insert(entryC, series, errorC)
    entryC <- Entry{
        Timestamp: time.Now(),
        Value: value,
    }
    close(entryC)
    return <-errorC
}
