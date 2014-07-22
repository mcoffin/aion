package timedb

import (
    "bytes"
    "time"
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

type BucketStore struct {
    Duration time.Duration
    Granularity time.Duration
    Aggregators map[string]aggregate.Aggregator
    Multiplier float64
    aContext *aggregationContext
    buffers map[string]*bytes.Buffer
    encoders map[string]*bucket.BucketEncoder
    timeBuffer *bytes.Buffer
    timeEncoder *bucket.BucketEncoder
}

func (self *BucketStore) resetAggregators() {
    for _, a := range self.Aggregators {
        a.Reset()
    }
}

func (self BucketStore) marshalFloat(v float64) int64 {
    return int64(v * self.Multiplier)
}

func (self *BucketStore) flushAggregators() {
    timeEncoder.WriteInt(self.aContext.start.Unix())
    for name, enc := range encoders {
        enc.WriteInt(self.marshalFloat(self.Aggregators[name].Value()))
    }
    self.aContext.reset(self.aContext.end)
    self.resetAggregators()
}

func (self *BucketStore) Insert(series uuid.UUID, entry Entry, aggregator string) error {
    // First-time setup
    if self.aContext == nil {
        start := entry.Timestamp.Truncate(self.Duration)
        self.aContext = newAggregationContext(start, self.Granularity)
        self.resetAggregators()
        if self.encoders == nil {
            self.encoders = make(map[string]*bucket.BucketEncoder)
        }
        self.timeBuffer = new(bytes.Buffer)
        self.timeEncoder = bucket.NewBucketEncoder(start, self.timeBuffer)
        for name, _ := range self.Aggregators {
            self.buffers[name] = new(bytes.Buffer)
            self.encoders[name] = bucket.NewBucketEncoder(self.marshalFloat(entry.Value), buf)
        }
    } else {
        if self.aContext.needsRollup(entry.Timestamp) {
            self.flushAggregators()
        }
    }
    // Add the new value to the correct aggregator
    self.Aggregators[aggregator].Add(entry.Value)
    return nil
}
