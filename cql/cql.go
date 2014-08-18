package cql

import (
	"errors"
	"fmt"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion"
	"github.com/gocql/gocql"
)

// Private type marshalled out by gocql
type cqlAttribsMap map[string][]byte

// Converts this map into the aion.EncodedBucketAttribute array expected by the rest of Aion
func (self cqlAttribsMap) encodedAttributes() []aion.EncodedBucketAttribute {
	ret := make([]aion.EncodedBucketAttribute, len(self))
	i := 0
	for name, data := range self {
		ret[i] = aion.EncodedBucketAttribute{
			Name: name,
			Data: data,
		}
		i++
	}
	return ret
}

// A CQLRepository stores bucket data in a CQL capable database
type CQLRepository struct {
	ColumnFamily string
	Session      *gocql.Session
}

// CQLRepository implements the BucketRepository interface
func (self CQLRepository) Get(series uuid.UUID, duration time.Duration, start time.Time, attributes []string) ([]aion.EncodedBucketAttribute, error) {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return nil, err
	}
	queryStr := fmt.Sprintf("SELECT attribs FROM %d WHERE series = ? and time = ?", self.ColumnFamily)
	iter := self.Session.Query(queryStr, seriesUUID, start).Iter()
	var attribs cqlAttribsMap
	for iter.Scan(&attribs) {
		return attribs.encodedAttributes(), iter.Close()
	}
	return nil, errors.New("cql: bucket not found")
}

// CQLRepository implements the BucketRepository interface
func (self CQLRepository) Put(series uuid.UUID, duration time.Duration, start time.Time, attributes []aion.EncodedBucketAttribute) error {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return err
	}
	attribMap := make(map[string][]byte, len(attributes))
	for _, encodedAttribute := range attributes {
		attribMap[encodedAttribute.Name] = encodedAttribute.Data
	}
	queryStr := fmt.Sprintf("INSERT INTO %s (series, time, attribs) VALUES (?, ?, ?)", self.ColumnFamily)
	return self.Session.Query(queryStr, seriesUUID, start, attribMap).Exec()
}

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
