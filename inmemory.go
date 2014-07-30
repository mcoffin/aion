package timedb

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/bucket"
	"time"
)

type inMemoryBlockBuilderContext struct {
	buffer  bytes.Buffer
	encoder *bucket.BucketEncoder
}

type inMemoryBucketBuilderContext struct {
	baseline int64
	end      time.Time
	contexts map[string]inMemoryBlockBuilderContext
}

func (self *inMemoryBucketBuilderContext) addEntry(entry Entry, builder *InMemoryBucketBuilder) {
	// First write the time
	timeContext := self.contexts["times"]
	if timeContext.encoder == nil {
		timeContext.encoder = bucket.NewBucketEncoder(self.end.Add(-builder.Duration), &timeContext.buffer)
	}
	timeContext.encoder.WriteInt(entry.Timestamp.Unix())
	// Then write all remaining attributes
	for name, value := range entry.Attributes {
		context := self.contexts[name]
		iValue := int64(value * builder.Multiplier)
		if context.encoder == nil {
			self.baseline = iValue
			context.encoder = bucket.NewBucketEncoder(iValue, &context.buffer)
		}
		context.encoder.WriteInt(iValue)
	}
}

func (self *inMemoryBucketBuilderContext) query(start, end time.Time, attributes []string, builder *InMemoryBucketBuilder) EntryReader {
	decoders := map[string]*bucket.BucketDecoder{}
	decoders["times"] = bucket.NewBucketDecoder(builder.bucketStartTime(start), &self.contexts["times"].buffer)
	for _, a := range attributes {
		decoders[a] = bucket.NewBucketDecoder(self.baseline, &self.contexts[a].buffer)
	}

}

type InMemoryBucketBuilder struct {
	Duration   time.Duration
	Multiplier float64
	contexts   map[string]map[time.Time]*inMemoryBucketBuilderContext
}

func (self InMemoryBucketBuilder) bucketStartTime(t time.Time) time.Time {
	return t.Truncate(self.Duration)
}

func (self *InMemoryBucketBuilder) seriesMap(series uuid.UUID) map[time.Time]*inMemoryBucketBuilderContext {
	ret := self.contexts[series.String()]
	if ret == nil {
		ret = map[time.Time]*inMemoryBucketBuilderContext{}
		self.contexts[series.String()] = ret
	}
	return ret
}

func (self *InMemoryBucketBuilder) Insert(series uuid.UUID, entry Entry) error {
	// First, find or create a context for desired bucket
	seriesMap := self.seriesMap(series)
	start := self.bucketStartTime(entry.Timestamp)
	context := seriesMap[start]
	if context == nil {
		context = &inMemoryBucketBuilderContext{
			end:      start.Add(self.Duration),
			contexts: map[string]inMemoryBlockBuilderContext{},
		}
		seriesMap[start] = context
	}
	// Then write the desired entry to the context
	context.addEntry(entry, self)
	return nil
}

func (self *InMemoryBucketBuilder) Query(series uuid.UUID, start, end time.Time, attributes []string) (EntryReader, error) {
	seriesContext := self.contexts[series.String()]
	var ret (func([]Entry) (int, error))
	ret = func(entries []Entry) (int, error) {
	}
	return queryFunc(ret), nil
}

func (self *InMemoryBucketBuilder) WriteBuckets(repo BucketRepository) error {
	// TODO
	return nil
}
