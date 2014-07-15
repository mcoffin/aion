package timedb

import (
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
)

type CQLCache struct {
    Session *gocql.Session
}

// CQLCache implements the QueryLevel interface
func (self *CQLCache) Insert(entries chan Entry, series uuid.UUID, success chan error) {
    for {
        entry, more := <-entries
        if more {
            seriesUUID, err := gocql.UUIDFromBytes(series)
            if err != nil {
                success <- err
                return
            }
            err = self.Session.Query("INSERT INTO cache (series, time, value) VALUES (?, ?, ?)", seriesUUID, entry.Timestamp, entry.Value).Exec()
            if err != nil {
                success <- err
                return
            }
        } else {
            success <- nil
            return
        }
    }
}

// CQLCache implements the Querier interface - It will ignore the granularity and aggregation arguments
func (self *CQLCache) Query(entries chan Entry, series uuid.UUID, granularity time.Duration, aggregation string, start time.Time, end time.Time, success chan error) {
    seriesUUID, err := gocql.UUIDFromBytes(series)
    if err != nil {
        success <- err
        return
    }
    iter := self.Session.Query("SELECT time, value FROM cache WHERE series = ? AND time > ? AND time < ?", seriesUUID, start, end).Iter()
    var timestamp time.Time
    var value float64
    for iter.Scan(&timestamp, &value) {
        entries <- Entry{
            Timestamp: timestamp,
            Value: value,
        }
    }
    close(entries)
    err = iter.Close()
    success <- err
}
