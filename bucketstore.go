package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion/bucket"
	"time"
)

const (
	TimeAttribute = "times"
)

// An EncodedAttribute represents a series of encoded numbers
// for example, all of the "avg" numbers in a given bucket
type EncodedBucketAttribute struct {
	Name string
	Data []byte
}

// A BucketRepository represents a persistent store for buckets (probably on disc somewhere)
type BucketRepository interface {
	Querier
	Put(series uuid.UUID, granularity time.Duration, start time.Time, attributes []EncodedBucketAttribute) error
}

// A BucketBuilder represents a cache for buckets while they are being built
type BucketBuilder interface {
	SeriesStore
	BucketsToWrite(series uuid.UUID) []time.Time
	Get(series uuid.UUID, start time.Time) ([]EncodedBucketAttribute, error)
	Delete(series uuid.UUID, start time.Time)
}

// A BucketStore represents a composition of a BucketRepository and a BucketBuilder
// to make a fully persistent bucket storage scheme
type BucketStore struct {
	Granularity time.Duration
	Repository  BucketRepository
	Builder     BucketBuilder
}

// BucketStore implements the SeriesStore interface
func (self *BucketStore) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// Query from memory and then from the repo
	queriers := []Querier{self.Repository, self.Builder}
	last := start
	for i, q := range queriers {
		err := ForAllQuery(series, last, end, attributes, q, func(e Entry) {
			last = e.Timestamp
			e.Attributes["querier"] = float64(i)
			entries <- e
		})
		if err != nil {
			errors <- err
			return
		}
		last = last.Add(time.Second)
	}
}

// BucketStore implements the SeriesStore interface
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

// Convenience function for creating an EntryReader from a set of BucketDecoders and their surrounding context
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
