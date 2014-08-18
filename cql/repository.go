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

// A Repository stores bucket data in a CQL capable database
type Repository struct {
	ColumnFamily string
	Session      *gocql.Session
}

// Repository implements the BucketRepository interface
func (self Repository) Get(series uuid.UUID, duration time.Duration, start time.Time, attributes []string) ([]aion.EncodedBucketAttribute, error) {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return nil, err
	}
	queryStr := fmt.Sprintf("SELECT attribs FROM %d WHERE series = ? and duration = ? and time = ?", self.ColumnFamily)
	iter := self.Session.Query(queryStr, seriesUUID, int64(duration.Seconds()), start).Iter()
	var attribs cqlAttribsMap
	for iter.Scan(&attribs) {
		return attribs.encodedAttributes(), iter.Close()
	}
	return nil, errors.New("cql: bucket not found")
}

// Repository implements the BucketRepository interface
func (self Repository) Put(series uuid.UUID, duration time.Duration, start time.Time, attributes []aion.EncodedBucketAttribute) error {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return err
	}
	attribMap := make(map[string][]byte, len(attributes))
	for _, encodedAttribute := range attributes {
		attribMap[encodedAttribute.Name] = encodedAttribute.Data
	}
	queryStr := fmt.Sprintf("INSERT INTO %s (series, duration, time, attribs) VALUES (?, ?, ?, ?)", self.ColumnFamily)
	return self.Session.Query(queryStr, seriesUUID, int64(duration.Seconds()), start, attribMap).Exec()
}
