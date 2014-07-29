package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"bytes"
	"github.com/FlukeNetworks/timedb/bucket"
	"time"
)

type inMemoryBlockBuilderContext struct {
	buffer bytes.Buffer
	encoder *bucket.BucketEncoder
}

type inMemoryBucketBuilderContext struct {
	end time.Time
	contexts map[string]*inMemoryBlockBuilderContext
}

type InMemoryBucketBuilder struct {
	Duration time.Duration
	Multiplier float64
	contexts map[string]map[time.Time]*inMemoryBucketBuilderContext
}

func (self *InMemoryBucketBuilder) Insert(series uuid.UUID, entry Entry) error {
	// TODO
	return nil
}

func (self *InMemoryBucketBuilder) Query(series uuid.UUID, start, end time.Time, attributes []string) (EntryReader, error) {
	// TODO
	return nil, nil
}
