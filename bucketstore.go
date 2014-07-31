package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"time"
)

const (
	TimeAttribute = "times"
)

type EncodedBucketAttribute struct {
	Name string
	Data []byte
}

type EncodedBucket struct {
	Start      time.Time
	Attributes []EncodedBucketAttribute
}

type BucketRepository interface {
	Put(series uuid.UUID, granularity time.Duration, start time.Time, attributes []EncodedBucketAttribute) error
	Get(series uuid.UUID, start time.Time) ([]EncodedBucketAttribute, error)
	Query(series uuid.UUID, start, end time.Time, attributes []string) ([]EncodedBucket, error)
}

type bucketRepositoryQuerier struct {
	repo BucketRepository
}

func (self bucketRepositoryQuerier) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// TODO
}

type BucketBuilder interface {
	SeriesStore
	BucketsToWrite(series uuid.UUID) []time.Time
	Get(series uuid.UUID, start time.Time) ([]EncodedBucketAttribute, error)
}

type BucketStore struct {
	Granularity time.Duration
	Repository  BucketRepository
	Builder     BucketBuilder
}

func (self *BucketStore) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// Query from memory and then from the repo
	queriers := []Querier{self.Builder, bucketRepositoryQuerier{self.Repository}}
	for _, q := range queriers {
		q.Query(series, start, end, attributes, entries, errors)
	}
}

func (self *BucketStore) Insert(series uuid.UUID, entry Entry) error {
	err := self.Builder.Insert(series, entry)
	if err != nil {
		return err
	}
	writeTimes := self.Builder.BucketsToWrite(series)
	for _, t := range writeTimes {
		data, err := self.Builder.Get(series, t)
		if err != nil {
			return err
		}
		err = self.Repository.Put(series, self.Granularity, t, data)
		if err != nil {
			return err
		}
	}
	return nil
}
