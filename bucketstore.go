package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/bucket"
	"time"
)

const (
	TimeAttribute = "times"
)

type EncodedBucketAttribute struct {
	Name string
	Data []byte
}

type BucketRepository interface {
	Querier
	Put(series uuid.UUID, granularity time.Duration, start time.Time, attributes []EncodedBucketAttribute) error
}

type BucketBuilder interface {
	SeriesStore
	BucketsToWrite(series uuid.UUID) []time.Time
	Get(series uuid.UUID, start time.Time) ([]EncodedBucketAttribute, error)
	Delete(series uuid.UUID, start time.Time)
}

type BucketStore struct {
	Granularity time.Duration
	Repository  BucketRepository
	Builder     BucketBuilder
}

func (self *BucketStore) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// Query from memory and then from the repo
	queriers := []Querier{self.Repository, self.Builder}
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
		if len(data) <= 0 {
			self.Builder.Delete(series, t)
			continue
		}
		err = self.Repository.Put(series, self.Granularity, t, data)
		if err != nil {
			return err
		} else {
			self.Builder.Delete(series, t)
		}
	}
	return nil
}

func bucketEntryReader(series uuid.UUID, multiplier float64, decs map[string]*bucket.BucketDecoder, attributes []string) EntryReader {
	ret := func(entries []Entry) (int, error) {
		iBuf := make([]int64, len(entries))
		n, err := decs[TimeAttribute].Read(iBuf)
		iBuf = iBuf[:n]
		if n > 0 {
			for i, v := range iBuf {
				entries[i].Timestamp = time.Unix(v, 0)
			}
			mult := 1 / multiplier
			for _, a := range attributes {
				decs[a].Read(iBuf)
				for i, v := range iBuf {
					entries[i].Attributes[a] = float64(v) * mult
				}
			}
		}
		return n, err
	}
	return entryReaderFunc(ret)
}
