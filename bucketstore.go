package timedb

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/bucket"
	"time"
)

type BucketStoreContext struct {
	Buffer  bytes.Buffer
	encoder *bucket.BucketEncoder
}

func NewBucketStoreContext(baseline int64) *BucketStoreContext {
	ctx := BucketStoreContext{}
	ctx.encoder = bucket.NewBucketEncoder(baseline, &ctx.Buffer)
	return &ctx
}

type SeriesBucketStoreContext struct {
	Contexts map[string]*BucketStoreContext
	Baseline int64
	End      time.Time
}

func NewSeriesBucketStoreContext(entry Entry, store *BucketStore) *SeriesBucketStoreContext {
	var baseline int64
	for _, v := range entry.Attributes {
		baseline = marshalFloat64(v, store.Multiplier)
		break
	}
	ctx := SeriesBucketStoreContext{
		Contexts: map[string]*BucketStoreContext{},
		Baseline: baseline,
		End:      store.BucketTime(entry.Timestamp).Add(store.Duration),
	}
	for k, _ := range entry.Attributes {
		ctx.Contexts[k] = NewBucketStoreContext(ctx.Baseline)
	}
	ctx.Contexts["times"] = NewBucketStoreContext(store.BucketTime(entry.Timestamp).Unix())
	return &ctx
}

func (self SeriesBucketStoreContext) Start(store *BucketStore) time.Time {
	return self.End.Add(-store.Duration)
}

func (self *SeriesBucketStoreContext) WriteEntry(entry Entry, store *BucketStore) {
	for k, v := range entry.Attributes {
		self.Contexts[k].encoder.WriteInt(marshalFloat64(v, store.Multiplier))
	}
	self.Contexts["times"].encoder.WriteInt(entry.Timestamp.Unix())
}

func (self *SeriesBucketStoreContext) Close() {
	for _, ctx := range self.Contexts {
		ctx.encoder.Close()
	}
}

type BucketRepository interface {
	Put(series uuid.UUID, context *SeriesBucketStoreContext, store *BucketStore) error
}

type BucketStore struct {
	Duration   time.Duration
	Multiplier float64
	Repository BucketRepository
	contexts   map[string]*SeriesBucketStoreContext
}

func (self BucketStore) BucketTime(t time.Time) time.Time {
	return t.Truncate(self.Duration)
}

func (self *BucketStore) Init() {
	self.contexts = map[string]*SeriesBucketStoreContext{}
}

func (self *BucketStore) Insert(series uuid.UUID, entry Entry) error {
	seriesStr := series.String()
	seriesContext := self.contexts[seriesStr]
	// Check for special bucket conditions
	if seriesContext == nil {
		seriesContext = NewSeriesBucketStoreContext(entry, self)
		self.contexts[seriesStr] = seriesContext
	} else if entry.Timestamp.After(seriesContext.End) {
		seriesContext.Close()
		err := self.Repository.Put(series, seriesContext, self)
		if err != nil {
			return err
		}
		self.contexts[seriesStr] = nil
		return self.Insert(series, entry)
	}
	// Write the entry
	seriesContext.WriteEntry(entry, self)
	return nil
}

func marshalFloat64(v float64, multiplier float64) int64 {
	return int64(v * multiplier)
}
