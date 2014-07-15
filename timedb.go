package timedb

import (
    "time"
    "code.google.com/p/go-uuid/uuid"
)

// One entry in a time series
type Entry struct {
    Timestamp time.Time
    Value float64
}

// Represents the storage scheme for a type of block
type QueryLevel interface {
    Insert(entries chan Entry, series uuid.UUID, success chan error)
    Query(entries chan Entry, series uuid.UUID, granularity time.Duration, aggregation string, start time.Time, end time.Time, success chan error)
}

// Root of the top-level API, contains all information
// for the configuration of a single TimeDB instance
type TimeDB struct {
    QueryLevels []QueryLevel
}

// Creates a new TimeDB from the user-defined
// query levels
func NewTimeDB(qLevels ...QueryLevel) *TimeDB {
    db := &TimeDB{
        QueryLevels: qLevels,
    }
    return db
}

// Convenience method to insert a new data point in to the first QueryLevel of the TimeDB
// (usually the cache)
func (self *TimeDB) Put(series uuid.UUID, value float64, t time.Time) error {
    entryC := make(chan Entry) // No buffer because we're only sending one value
    errorC := make(chan error)
    go self.QueryLevels[0].Insert(entryC, series, errorC)
    ent := Entry{
        Timestamp: time.Now(),
        Value: value,
    }
    var err error
    select {
    case entryC <- ent:
    case err = <-errorC:
    }
    close(entryC)
    return err
}
