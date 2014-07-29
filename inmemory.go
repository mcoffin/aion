package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"time"
)

type InMemoryBucketBuilder struct {
}

func (self *InMemoryBucketBuilder) Insert(series uuid.UUID, entry Entry) error {
	// TODO
	return nil
}

func (self *InMemoryBucketBuilder) Query(series uuid.UUID, start, end time.Time, attributes []string) (EntryReader, error) {
	// TODO
	return nil, nil
}
