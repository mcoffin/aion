package timedb

import (
    "bytes"
    "io"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/FlukeNetworks/timedb/aggregate"
    "github.com/FlukeNetworks/timedb/bucket"
)

type aggregationContext struct {
    start, end time.Time
}

func newAggregationContext(start time.Time, duration time.Duration) *aggregationContext {
    ac := &aggregationContext{
        start: start.Truncate(duration),
    }
    ac.end = ac.start.Add(duration)
    return ac
}

func (self aggregationContext) needsRollup(now time.Time) bool {
    return now.After(self.end)
}

func (self *aggregationContext) reset(start time.Time, duration time.Duration) {
    self.start = start.Truncate(duration)
    self.end = self.start.Add(duration)
}

type BucketStorer interface {
    StoreBucket(store *BucketStore, times *bytes.Buffer, values []*bytes.Buffer, start time.Time, baseline float64, series uuid.UUID) error
}

type BucketStore struct {
    Duration time.Duration
    Granularity time.Duration
    Aggregators map[string]aggregate.Aggregator
    Multiplier float64
    Storer BucketStorer
    aContext *aggregationContext
    buffers map[string]*bytes.Buffer
    encoders map[string]*valueEncoder
    timeBuffer *bytes.Buffer
    timeEncoder *bucket.BucketEncoder
    start time.Time
}

func (self *BucketStore) resetAggregators() {
    for _, a := range self.Aggregators {
        a.Reset()
    }
}

func (self *BucketStore) flushAggregators() {
    self.timeEncoder.WriteInt(self.aContext.start.Unix())
    for name, enc := range self.encoders {
        enc.Write(self.Aggregators[name].Value())
    }
    self.aContext.reset(self.aContext.end, self.Granularity)
    self.resetAggregators()
}

func (self *BucketStore) Insert(series uuid.UUID, entry Entry, aggregator string) error {
    // First-time setup
    if self.aContext == nil {
        self.start = entry.Timestamp.Truncate(self.Duration)
        self.aContext = newAggregationContext(self.start, self.Granularity)
        self.resetAggregators()
        if self.encoders == nil {
            self.encoders = make(map[string]*valueEncoder)
        }
        self.timeBuffer = new(bytes.Buffer)
        self.timeEncoder = bucket.NewBucketEncoder(self.start.Unix(), self.timeBuffer)
        for name, _ := range self.Aggregators {
            buf := new(bytes.Buffer)
            self.buffers[name] = buf
            self.encoders[name] = newValueEncoder(entry.Value, self.Multiplier, buf)
        }
    } else {
        if self.aContext.needsRollup(entry.Timestamp) {
            self.flushAggregators()
        }
        if entry.Timestamp.After(self.start.Add(self.Duration)) {
        }
    }
    // Add the new value to the correct aggregator
    self.Aggregators[aggregator].Add(entry.Value)
    return nil
}

type valueEncoder struct {
    enc *bucket.BucketEncoder
    multiplier float64
}

func newValueEncoder(baseline float64, multiplier float64, writer io.Writer) *valueEncoder {
    enc := &valueEncoder{
        multiplier: multiplier,
        enc: bucket.NewBucketEncoder(int64(baseline * multiplier), writer),
    }
    return enc
}

func (self *valueEncoder) Write(v float64) {
    self.enc.WriteInt(int64(v * self.multiplier))
}

func (self *valueEncoder) Close() {
    self.enc.Close()
}
