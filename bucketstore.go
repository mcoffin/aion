package aion

import (
	"bytes"
	"time"

	"github.com/FlukeNetworks/aion/bucket"
	"github.com/google/btree"

	"code.google.com/p/go-uuid/uuid"
)

const (
	TimeAttribute = "times"
)

type BucketStore struct {
	Granularity, Duration time.Duration
	Multiplier            float64
	Source                Querier
	contexts              map[string]*btree.BTree
}

type memoryBucketAttribute struct {
	buffer bytes.Buffer
	enc    *bucket.BucketEncoder
}

type memoryBucket struct {
	start    time.Time
	contexts map[string]*memoryBucketAttribute
}

// memoryBucket implements the btree.Item iterface
func (a memoryBucket) Less(b btree.Item) bool {
	other := b.(memoryBucket)
	return a.start.Before(other.start)
}

func (self BucketStore) bucketStartTime(t time.Time) time.Time {
	return t.Truncate(self.Duration)
}

// BucketStore implements the SeriesStore interface
func (self *BucketStore) Insert(series uuid.UUID, entry Entry) error {
	seriesStr := series.String()

	// If we don't have a tree for the series, construct one
	tree := self.contexts[seriesStr]
	if tree == nil {
		tree = btree.New(2)
		self.contexts[seriesStr] = tree
	}

	// If we don't have a memory bucket for this entry, allocate one
	var bkt memoryBucket
	bktKey := memoryBucket{start: self.bucketStartTime(entry.Timestamp)}
	item := tree.Get(bktKey)
	if item == nil {
		bkt = memoryBucket{
			start:    bktKey.start,
			contexts: map[string]*memoryBucketAttribute{},
		}
		tree.ReplaceOrInsert(bkt)
		// TODO: load bucket from source and/or self's persistent cache
	} else {
		bkt = item.(memoryBucket)
	}

	// TODO
	return nil
}

func (self *BucketStore) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// TODO
}
