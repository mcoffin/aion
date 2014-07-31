package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"time"
)

type BucketRepository interface {
	Querier
}

type BucketBuilder interface {
	SeriesStore
	WriteBuckets(repo BucketRepository) error
}

type BucketStore struct {
	Duration   time.Duration
	Multiplier float64
	Repository BucketRepository
	Builder    BucketBuilder
}

func (self *BucketStore) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// Query from memory and then from the repo
	queriers := []Querier{self.Builder, self.Repository}
	for _, q := range queriers {
		q.Query(series, start, end, attributes, entries, errors)
	}
}

func (self *BucketStore) Insert(series uuid.UUID, entry Entry) error {
	err := self.Builder.Insert(series, entry)
	if err != nil {
		return err
	}
	return self.Builder.WriteBuckets(self.Repository)
}
