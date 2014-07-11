package timedb

import (
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
)

type CQLCache struct {
    Session *gocql.Session
}

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
