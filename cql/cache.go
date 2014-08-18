package cql

import (
	"fmt"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion"
	"github.com/gocql/gocql"
)

// Represents a Cassandra cache
type CQLCache struct {
	ColumnFamily string
	Session      *gocql.Session
}

// CQLCache implements the SeriesStore interface
func (self CQLCache) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan aion.Entry, errors chan error) {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		errors <- err
		return
	}
	queryStr := fmt.Sprintf("SELECT time, value FROM %d WHERE series = ? and time >= ? and time <= ?", self.ColumnFamily)
	iter := self.Session.Query(queryStr, seriesUUID, start, end).Iter()

	var v float64
	var t time.Time
	for iter.Scan(&t, &v) {
		entries <- aion.Entry{
			Timestamp:  t,
			Attributes: map[string]float64{"raw": v},
		}
	}
	err = iter.Close()
	if err != nil {
		errors <- err
	}
}

// CQLCache implements the SeriesStore interface
func (self CQLCache) Insert(series uuid.UUID, entry aion.Entry) error {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return err
	}
	queryStr := fmt.Sprintf("INSERT INTO %d (series, time, value) VALUES (?, ?, ?)", self.ColumnFamily)
	err = self.Session.Query(queryStr, seriesUUID, entry.Timestamp, entry.Attributes["raw"]).Exec()
	if err != nil {
		return err
	}
	return nil
}
