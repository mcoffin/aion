package aion

import (
	"bytes"
	"io"
	"time"

	"github.com/FlukeNetworks/aion/bucket"
	"github.com/google/btree"

	"code.google.com/p/go-uuid/uuid"
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
	Get(series uuid.UUID, duration time.Duration, start time.Time, attributes []string) ([]EncodedBucketAttribute, error)
	Put(series uuid.UUID, duration time.Duration, start time.Time, attributes []EncodedBucketAttribute) error
}

type BucketStore struct {
	Duration   time.Duration
	Multiplier float64
	Source     Querier
	Repository BucketRepository
	Filter     Filter
	contexts   map[string]*btree.BTree
}

type memoryBucketAttribute struct {
	buffer bytes.Buffer
	enc    *bucket.BucketEncoder
}

func newMemoryBucketAttribute(baseline int64) *memoryBucketAttribute {
	ret := &memoryBucketAttribute{}
	ret.enc = bucket.NewBucketEncoder(baseline, &ret.buffer)
	return ret
}

type memoryBucket struct {
	start    time.Time
	contexts map[string]*memoryBucketAttribute
}

func (self memoryBucket) writeEntry(entry Entry, multiplier float64) {
	self.contexts[TimeAttribute].enc.WriteInt(entry.Timestamp.Unix())
	for k, v := range entry.Attributes {
		self.contexts[k].enc.WriteInt(int64(v * multiplier))
	}
}

func (self memoryBucket) populate(attribs []EncodedBucketAttribute) {
	for _, a := range attribs {
		buf := bytes.NewBuffer(a.Data)
		mAttrib := &memoryBucketAttribute{
			buffer: *buf,
		}
		var baseline int64
		switch a.Name {
		case TimeAttribute:
			baseline = self.start.Unix()
		default:
			baseline = 0
		}
		mAttrib.enc = bucket.NewBucketEncoder(baseline, &mAttrib.buffer)
	}
}

func (self memoryBucket) verifyContexts(entry Entry) {
	timeAttribute := self.contexts[TimeAttribute]
	if timeAttribute == nil {
		timeAttribute = newMemoryBucketAttribute(self.start.Unix())
		self.contexts[TimeAttribute] = timeAttribute
	}
	for k, _ := range entry.Attributes {
		a := self.contexts[k]
		if a == nil {
			a = newMemoryBucketAttribute(0)
			self.contexts[k] = a
		}
	}
}

// memoryBucket implements the btree.Item iterface
func (a memoryBucket) Less(b btree.Item) bool {
	other := b.(memoryBucket)
	return a.start.Before(other.start)
}

func NewBucketStore(duration time.Duration, multiplier float64) *BucketStore {
	return &BucketStore{
		Duration:   duration,
		Multiplier: multiplier,
		contexts:   map[string]*btree.BTree{},
	}
}

func (self BucketStore) bucketStartTime(t time.Time) time.Time {
	return t.Truncate(self.Duration)
}

func (self *BucketStore) getOrCreateTree(series uuid.UUID) *btree.BTree {
	tree := self.contexts[series.String()]
	if tree == nil {
		tree = btree.New(2)
		self.contexts[series.String()] = tree
	}
	return tree
}

func (self *BucketStore) populateBucket(series uuid.UUID, bkt memoryBucket) error {
	// First, try to query the bucket out of the repository
	if self.Repository != nil {
		attribData, err := self.Repository.Get(series, self.Duration, bkt.start, nil)
		if err == nil {
			bkt.populate(attribData)
			return nil
		}
	}
	// If the bucket wasn't queried from the repo, then do it from source
	if self.Source != nil {
		gotBucket := false
		err := ForAllQuery(series, bkt.start, bkt.start.Add(self.Duration), nil, self.Source, func(e Entry) {
			gotBucket = true
			self.Filter.Insert(series, e) // TODO: handle that err doe
		})
		if gotBucket {
			return err
		}
	}
	// If the bucket wasn't in src or repo, then it's a new bucket!
	return nil
}

func (self *BucketStore) getOrCreateBucket(series uuid.UUID, entry Entry) memoryBucket {
	var bkt memoryBucket
	keyTime := entry.Timestamp
	tree := self.getOrCreateTree(series)
	bktKey := memoryBucket{start: self.bucketStartTime(keyTime)}
	item := tree.Get(bktKey)
	if item == nil {
		bkt = memoryBucket{
			start:    bktKey.start,
			contexts: map[string]*memoryBucketAttribute{},
		}
		tree.ReplaceOrInsert(bkt)
		err := self.populateBucket(series, bkt)
		if err != nil {
			panic(err) // TODO: actually handle this in a real manner
		}
	} else {
		bkt = item.(memoryBucket)
	}

	// Make sure that contexts exist for all possible attribs on the entry
	bkt.verifyContexts(entry)
	return bkt
}

// BucketStore implements the SeriesStore interface
func (self *BucketStore) Insert(series uuid.UUID, entry Entry) error {
	bkt := self.getOrCreateBucket(series, entry)
	bkt.writeEntry(entry, self.Multiplier)
	return nil
}

func (self *BucketStore) entryReader(series uuid.UUID, bkt memoryBucket, attributes []string) EntryReader {
	if bkt.contexts[TimeAttribute].enc == nil {
		return entryReaderFunc(func(entries []Entry) (int, error) {
			return 0, io.EOF
		})
	}
	bkt.contexts[TimeAttribute].enc.Close()
	decs := map[string]*bucket.BucketDecoder{
		TimeAttribute: bucket.NewBucketDecoder(bkt.start.Unix(), bytes.NewBuffer(bkt.contexts[TimeAttribute].buffer.Bytes())),
	}
	for _, a := range attributes {
		bkt.contexts[a].enc.Close()
		buf := bytes.NewBuffer(bkt.contexts[a].buffer.Bytes())
		decs[a] = bucket.NewBucketDecoder(0, buf)
	}
	return bucketEntryReader(series, self.Multiplier, decs, attributes)
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

func (self *BucketStore) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// Lose granularity
	start = start.Truncate(time.Second)
	// Loop through all buckets that we could possibly have
	for t := self.bucketStartTime(start); t.Before(end); t = t.Add(self.Duration) {
		bkt := self.getOrCreateBucket(series, Entry{Timestamp: t})
		entryBuf := make([]Entry, 1)
		for i, _ := range entryBuf {
			entryBuf[i].Attributes = map[string]float64{}
		}
		reader := self.entryReader(series, bkt, attributes)
		for {
			n, err := reader.ReadEntries(entryBuf)
			if n > 0 {
				for _, e := range entryBuf[:n] {
					if e.Timestamp.After(start) || e.Timestamp.Equal(start) {
						if e.Timestamp.After(end) {
							return
						}
						out := e
						out.Attributes = map[string]float64{}
						for k, v := range e.Attributes {
							out.Attributes[k] = v
						}
						entries <- out
					}
				}
			}
			if err != nil {
				break
			}
		}
	}
}
