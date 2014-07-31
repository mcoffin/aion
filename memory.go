package timedb

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/FlukeNetworks/timedb/bucket"
	"time"
)

type EntryReader interface {
	ReadEntries(entries []Entry) (int, error)
}

type entryReaderFunc (func([]Entry) (int, error))

func (self entryReaderFunc) ReadEntries(entries []Entry) (int, error) {
	return self(entries)
}

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

func (self *MemoryBucketBuilder) bucket(series uuid.UUID, entry Entry) (*memoryBucket, time.Time) {
	seriesMap := self.contexts[series.String()]
	if seriesMap == nil {
		seriesMap = map[time.Time]*memoryBucket{}
		self.contexts[series.String()] = seriesMap
	}
	startTime := self.bucketStartTime(entry.Timestamp)
	bkt := seriesMap[startTime]
	if bkt == nil {
		bkt = &memoryBucket{
			contexts: map[string]*memoryBucketAttribute{},
		}
		seriesMap[startTime] = bkt
	}
	return bkt, startTime
}

func (self *MemoryBucketBuilder) entryReader(start time.Time, bkt *memoryBucket, attributes []string) EntryReader {
	bkt.context(TimeAttribute).encoder.Close()
	decs := map[string]*bucket.BucketDecoder{
		TimeAttribute: bucket.NewBucketDecoder(start.Unix(), &bkt.context(TimeAttribute).buffer),
	}
	for _, a := range attributes {
		bkt.context(a).encoder.Close()
		decs[a] = bucket.NewBucketDecoder(0, &bkt.context(a).buffer)
	}
	ret := func(entries []Entry) (int, error) {
		iBuf := make([]int64, len(entries))
		n, err := decs[TimeAttribute].Read(iBuf)
		if n > 0 {
			for i, v := range iBuf {
				entries[i].Timestamp = time.Unix(v, 0)
			}
			mult := 1 / self.Multiplier
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

func (self *MemoryBucketBuilder) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	seriesStr := series.String()
	for t := self.bucketStartTime(start); !t.After(end); t = t.Add(self.Duration) {
		bucket := self.contexts[seriesStr][t]
		// If we don't have this bucket, then the query is done
		if bucket == nil {
			break
		}
		reader := self.entryReader(t, bucket, attributes)
		entryBuf := make([]Entry, 8)
		for i, _ := range entryBuf {
			entryBuf[i].Attributes = map[string]float64{}
		}
		for {
			n, err := reader.ReadEntries(entryBuf)
			if n > 0 {
				for _, e := range entryBuf[:n] {
					entries <- e
				}
			}
			if err != nil {
				break
			}
		}
		fmt.Println("Passed readloop")
	}
}

func (self *MemoryBucketBuilder) Insert(series uuid.UUID, entry Entry) error {
	bkt, _ := self.bucket(series, entry)
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
