package aion

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion/bucket"
	"time"
)

type memoryBucketAttribute struct {
	buffer  bytes.Buffer
	encoder *bucket.BucketEncoder
}

type memoryBucket struct {
	contexts map[string]*memoryBucketAttribute
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
	contexts   map[string]map[time.Time]*memoryBucket
}

func (self *MemoryBucketBuilder) Init() {
	self.contexts = map[string]map[time.Time]*memoryBucket{}
}

func (self MemoryBucketBuilder) bucketStartTime(t time.Time) time.Time {
	return t.Truncate(self.Duration)
}

func (self *MemoryBucketBuilder) bucket(series uuid.UUID, t time.Time) (*memoryBucket, time.Time) {
	seriesMap := self.contexts[series.String()]
	if seriesMap == nil {
		seriesMap = map[time.Time]*memoryBucket{}
		self.contexts[series.String()] = seriesMap
	}
	startTime := self.bucketStartTime(t)
	bkt := seriesMap[startTime]
	if bkt == nil {
		bkt = &memoryBucket{
			contexts: map[string]*memoryBucketAttribute{},
		}
		seriesMap[startTime] = bkt
	}
	return bkt, startTime
}

func (self *MemoryBucketBuilder) entryReader(series uuid.UUID, start time.Time, bkt *memoryBucket, attributes []string) EntryReader {
	bkt.context(TimeAttribute).encoder.Close()
	decs := map[string]*bucket.BucketDecoder{
		TimeAttribute: bucket.NewBucketDecoder(start.Unix(), &bkt.context(TimeAttribute).buffer),
	}
	for _, a := range attributes {
		bkt.context(a).encoder.Close()
		decs[a] = bucket.NewBucketDecoder(0, &bkt.context(a).buffer)
	}
	return bucketEntryReader(series, self.Multiplier, decs, attributes)
}

func (self *MemoryBucketBuilder) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	seriesStr := series.String()
	for t := self.bucketStartTime(start); t.Before(end); t = t.Add(self.Duration) {
		bucket := self.contexts[seriesStr][t]
		// If we don't have this bucket, then move on down the line
		if bucket == nil {
			continue
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
					entries <- e
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
		timeEncoder = bucket.NewBucketEncoder(entry.Timestamp.Unix(), &bkt.context(TimeAttribute).buffer)
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
	if seriesMap == nil || len(seriesMap) < 2 {
		return nil
	}
	var largest *time.Time
	for t, _ := range seriesMap {
		if largest == nil || t.After(*largest) {
			largest = &t
		}
	}
	ret := make([]time.Time, len(seriesMap)-1)
	i := 0
	for t, _ := range seriesMap {
		if t.Before(*largest) {
			ret[i] = t
			i++
		}
	}
	return ret
}

func (self *MemoryBucketBuilder) Get(series uuid.UUID, start time.Time) ([]EncodedBucketAttribute, error) {
	bkt := self.contexts[series.String()][start]
	if bkt == nil {
		return nil, nil
	}
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
	delete(seriesMap, t)
}
