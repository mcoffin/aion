package aion

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion/bucket"
	"github.com/google/btree"
	"io"
	"time"
)

type memoryBucketAttribute struct {
	buffer  bytes.Buffer
	encoder *bucket.BucketEncoder
}

type memoryBucket struct {
	start    time.Time
	contexts map[string]*memoryBucketAttribute
}

// memoryBucket implements the btree.Item interface
func (a *memoryBucket) Less(b btree.Item) bool {
	other := b.(*memoryBucket)
	return a.start.Before(other.start)
}

func (self *memoryBucket) context(attribute string) *memoryBucketAttribute {
	ctx := self.contexts[attribute]
	if ctx == nil {
		ctx = new(memoryBucketAttribute)
		self.contexts[attribute] = ctx
	}
	return ctx
}

type MemoryBucketBuilder struct {
	Duration   time.Duration
	Multiplier float64
	Source     Querier
	contexts   map[string]*btree.BTree
}

func (self *MemoryBucketBuilder) Init() {
	self.contexts = map[string]*btree.BTree{}
}

func (self MemoryBucketBuilder) bucketStartTime(t time.Time) time.Time {
	return t.Truncate(self.Duration)
}

func (self *MemoryBucketBuilder) bucket(series uuid.UUID, t time.Time) (*memoryBucket, time.Time) {
	seriesMap := self.contexts[series.String()]
	if seriesMap == nil {
		seriesMap = btree.New(2) // TODO: actually come up with a sensible degree
		self.contexts[series.String()] = seriesMap
	}
	startTime := self.bucketStartTime(t)
	bktKey := &memoryBucket{start: startTime}
	item := seriesMap.Get(bktKey)
	var bkt *memoryBucket
	if item == nil {
		bkt = &memoryBucket{
			start:    startTime,
			contexts: map[string]*memoryBucketAttribute{},
		}
		seriesMap.ReplaceOrInsert(bkt)

		// Insert data from querier
		// TODO: should handle errors
		// TODO: put this data through the filter, not directly in
		if self.Source != nil {
			ForAllQuery(series, startTime, startTime.Add(self.Duration), nil, self.Source, func(e Entry) {
				self.Insert(series, e)
			})
		}
	} else {
		bkt = item.(*memoryBucket)
	}
	return bkt, startTime
}

func (self *MemoryBucketBuilder) entryReader(series uuid.UUID, start time.Time, bkt *memoryBucket, attributes []string) EntryReader {
	if bkt.context(TimeAttribute).encoder == nil {
		return entryReaderFunc(func(entries []Entry) (int, error) {
			return 0, io.EOF
		})
	}
	bkt.context(TimeAttribute).encoder.Close()
	decs := map[string]*bucket.BucketDecoder{
		TimeAttribute: bucket.NewBucketDecoder(start.Unix(), bytes.NewBuffer(bkt.context(TimeAttribute).buffer.Bytes())),
	}
	for _, a := range attributes {
		bkt.context(a).encoder.Close()
		buf := bytes.NewBuffer(bkt.context(a).buffer.Bytes())
		decs[a] = bucket.NewBucketDecoder(0, buf)
	}
	return bucketEntryReader(series, self.Multiplier, decs, attributes)
}

func (self *MemoryBucketBuilder) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	// Lose the possible millisecond accuracy in the passed time
	start = start.Truncate(time.Second)
	seriesStr := series.String()
	for t := self.bucketStartTime(start); t.Before(end); t = t.Add(self.Duration) {
		tree := self.contexts[seriesStr]
		bktKey := &memoryBucket{start: t}
		var item btree.Item
		if tree != nil {
			item = tree.Get(bktKey)
		}
		var bucket *memoryBucket
		// If we don't have this bucket, then move on down the line
		if item == nil {
			bucket, _ = self.bucket(series, t)
			tree = self.contexts[seriesStr]
			defer tree.Delete(bktKey)
		} else {
			bucket = item.(*memoryBucket)
		}
		reader := self.entryReader(series, t, bucket, attributes)
		entryBuf := make([]Entry, 1)
		entryBackBuf := make([]Entry, len(entryBuf))
		for i, _ := range entryBuf {
			entryBuf[i].Attributes = map[string]float64{}
			entryBackBuf[i].Attributes = map[string]float64{}
		}
		for {
			n, err := reader.ReadEntries(entryBuf)
			tmp := entryBuf
			entryBuf = entryBackBuf
			entryBackBuf = tmp
			if n > 0 {
				for _, e := range entryBackBuf[:n] {
					if e.Timestamp.After(start) || e.Timestamp.Equal(start) {
						if e.Timestamp.After(end) {
							return
						}
						entries <- e
					}
				}
			}
			if err != nil {
				break
			}
		}
	}
}

func (self *MemoryBucketBuilder) Insert(series uuid.UUID, entry Entry) error {
	bkt, _ := self.bucket(series, entry.Timestamp)
	timeEncoder := bkt.context(TimeAttribute).encoder
	if timeEncoder == nil {
		timeEncoder = bucket.NewBucketEncoder(self.bucketStartTime(entry.Timestamp).Unix(), &bkt.context(TimeAttribute).buffer)
		bkt.context(TimeAttribute).encoder = timeEncoder
	}
	timeEncoder.WriteInt(entry.Timestamp.Unix())
	for name, value := range entry.Attributes {
		enc := bkt.context(name).encoder
		if enc == nil {
			enc = bucket.NewBucketEncoder(0, &bkt.context(name).buffer)
			bkt.context(name).encoder = enc
		}
		enc.WriteInt(int64(value * self.Multiplier))
	}
	return nil
}

func (self *MemoryBucketBuilder) BucketsToWrite(series uuid.UUID) []time.Time {
	seriesMap := self.contexts[series.String()]
	if seriesMap == nil || seriesMap.Len() < 2 {
		return nil
	}
	ret := make([]time.Time, seriesMap.Len()-1)
	largestBucket := seriesMap.DeleteMax().(*memoryBucket)
	i := 0
	seriesMap.AscendLessThan(largestBucket, btree.ItemIterator(func(item btree.Item) bool {
		t := item.(*memoryBucket).start
		ret[i] = t
		i++
		return true
	}))
	seriesMap.ReplaceOrInsert(largestBucket)
	return ret
}

func (self *MemoryBucketBuilder) Get(series uuid.UUID, start time.Time) ([]EncodedBucketAttribute, error) {
	item := self.contexts[series.String()].Get(&memoryBucket{start: start})
	if item == nil {
		return nil, nil
	}
	bkt := item.(*memoryBucket)
	ret := make([]EncodedBucketAttribute, len(bkt.contexts))
	i := 0
	for name, ctx := range bkt.contexts {
		ctx.encoder.Close()
		ret[i] = EncodedBucketAttribute{
			Name: name,
			Data: ctx.buffer.Bytes(),
		}
		i++
	}
	return ret, nil
}

func (self *MemoryBucketBuilder) Delete(series uuid.UUID, t time.Time) {
	seriesMap := self.contexts[series.String()]
	if seriesMap == nil {
		return
	}
	// TODO: set TTL on source data
	seriesMap.Delete(&memoryBucket{start: t})
}
