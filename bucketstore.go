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

func (self *SeriesBucketStoreContext) Query(start time.Time, end time.Time, attributes []string, store *BucketStore, entries chan Entry) error {
	timeContext := self.Contexts["times"]
	decs := map[string]*bucket.BucketDecoder{}
	decs["times"] = bucket.NewBucketDecoder(self.Start(store).Unix(), &timeContext.Buffer)
	for _, name := range attributes {
		ctx := self.Contexts[name]
		decs[name] = bucket.NewBucketDecoder(self.Baseline, &ctx.Buffer)
	}
	buf := make([]int64, 8)
	entryBuf := make([]Entry, len(buf))
	for {
		entriesRead := 0
		for name, dec := range decs {
			n, err := dec.Read(buf)
			entriesRead = n
			if err != nil {
				return err
			}
			// If statement outside of the loop for optimization purposes
			if name == "times" {
				for i, value := range buf[:n] {
					entryBuf[i].Timestamp = time.Unix(value, 0)
				}
			} else {
				for i, value := range buf[:n] {
					entryBuf[i].Attributes[name] = float64(value) * (1.0 / store.Multiplier)
				}
			}
		}
		for _, e := range entryBuf[:entriesRead] {
			entries <- e
		}
		if entriesRead < len(entryBuf) {
			break
		}
	}
	return nil
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

func (self *BucketStore) Query(series uuid.UUID, start time.Time, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	seriesStr := series.String()
	// TODO query shit off of disk
	err := self.contexts[seriesStr].Query(start, end, attributes, self, entries)
	if err != nil {
		errors <- err
		return
	}
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
