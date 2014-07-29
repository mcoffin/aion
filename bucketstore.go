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
	WriteBuckets(repo BucketRepository)
}

type BucketStore struct {
	Duration   time.Duration
	Multiplier float64
	Repository BucketRepository
	Builder    BucketBuilder
}

func (self *BucketStore) Query(series uuid.UUID, start, end time.Time, attributes []string) (EntryReader, error) {
	// Create reader for the cache
	builderReader, err := self.Builder.Query(series, start, end, attributes)
	if err != nil {
		return nil, err
	}
	// Create reader for the repository
	repoReader, err := self.Builder.Query(series, start, end, attributes)
	if err != nil {
		return nil, err
	}
	readers := []EntryReader{builderReader, repoReader}
	// start with the first reader (index = 0)
	index := 0
	var ret (func([]Entry) (int, error))
	ret = func(entries []Entry) (int, error) {
		// Read stuff from the current reader
		n, err := readers[index].ReadEntries(entries)
		// If we didn't get enough entries, try the next reader (if it exists)
		if n < len(entries) {
			index++
			if index < len(readers) {
				nextN, nextErr := ret(entries[n:])
				n += nextN
				err = nextErr
			}
		}
		return n, err
	}
	// Cast ret to type queryFunc for returning
	return queryFunc(ret), nil
}

func (self *BucketStore) Insert(series uuid.UUID, entry Entry) error {
	err := self.Builder.Insert(series, entry)
	if err != nil {
		return err
	}
	return self.Builder.WriteBuckets(self.Repository)
}
