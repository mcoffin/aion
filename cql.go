package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/gocql/gocql"
	"time"
)

// Represents a Cassandra cache
type CQLCache struct {
	Session *gocql.Session
}

// CQLCache implements the SeriesStore interface
func (self *CQLCache) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		errors <- err
		return
	}
	iter := self.Session.Query("SELECT time, value FROM cache WHERE series = ? and time >= ? and time <= ?", seriesUUID, start, end).Iter()

	var v float64
	var t time.Time
	for iter.Scan(&t, &v) {
		entries <- Entry{
			Timestamp: t,
			Attributes: map[string]float64{"raw": v},
		}
	}
	err = iter.Close()
	if err != nil {
		errors <- err
	}
}

// CQLCache implements the SeriesStore interface
func (self *CQLCache) Insert(series uuid.UUID, entry Entry) error {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return err
	}
	err = self.Session.Query("INSERT INTO cache (series, time, value) VALUES (?, ?, ?)", seriesUUID, entry.Timestamp, entry.Attributes["raw"]).Exec()
	if err != nil {
		return err
	}
	return nil
}
