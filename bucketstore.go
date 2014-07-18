package timedb

import (
    "bytes"
    "errors"
    "io"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/FlukeNetworks/timedb/bucket"
    "github.com/FlukeNetworks/timedb/aggregate"
)

type BucketStorer interface {
    StoreBucket(store *BucketStore, times *bytes.Buffer, values []*bytes.Buffer, start time.Time, baseline float64, series uuid.UUID) error
}

// A representation of a backing store that keeps data in blocks and buckets
type BucketStore struct {
    Duration time.Duration
    Granularity time.Duration
    Aggregations []string
    Multiplier float64
    Storer BucketStorer
}

func (self *BucketStore) createAggregators() ([]aggregate.Aggregator, error) {
    aggregators := make([]aggregate.Aggregator, len(self.Aggregations))
    var err error
    for i, aggregation := range self.Aggregations {
        aggregators[i], err = aggregate.NewAggregator(aggregation)
        if err != nil {
            return aggregators, err
        }
    }
    return aggregators, nil
}

func (self *BucketStore) bucketIndex(aggregation string) (int, error) {
    for i, a := range self.Aggregations {
        if a == aggregation {
            return i, nil
        }
    }
    return 0, errors.New("Can't find aggregation")
}

func (self *BucketStore) Insert(entries chan Entry, series uuid.UUID, success chan error) {
    aggregators, err := self.createAggregators()
    if err != nil {
        success <- err
        return
    }

    tBuf := &bytes.Buffer{}
    var tEnc *timeEncoder
    vBufs := make([]*bytes.Buffer, len(aggregators))
    vEncs := make([]*valueEncoder, len(aggregators))
    isFirst := true
    var start time.Time
    var rollupStart, rollupEnd time.Time
    var baseline float64
    for {
        entry, more := <-entries
        if more {
            if isFirst {
                start = entry.Timestamp.Truncate(self.Duration)
                tEnc = newTimeEncoder(start, tBuf)
                rollupStart = entry.Timestamp.Truncate(self.Granularity)
                rollupEnd = rollupStart.Add(self.Granularity)

                baseline = entry.Value
                for i, _ := range aggregators {
                    vBufs[i] = &bytes.Buffer{}
                    vEncs[i] = newValueEncoder(baseline, self.Multiplier, vBufs[i])
                }
            }
            if (entry.Timestamp.After(rollupEnd) || entry.Timestamp.Equal(rollupEnd)) && !isFirst {
                tEnc.Write(rollupStart)
                for i, aggregator := range aggregators {
                    vEncs[i].Write(aggregator.Value())
                    aggregator.Reset()
                }
                rollupStart = entry.Timestamp.Truncate(self.Granularity)
                rollupEnd = rollupStart.Add(self.Granularity)
            }
            for _, aggregator := range aggregators {
                aggregator.Add(entry.Value)
            }
            isFirst = false
        } else {
            tEnc.Write(rollupStart)
            for i, aggregator := range aggregators {
                vEncs[i].Write(aggregator.Value())
            }
            tEnc.Close()
            for _, enc := range vEncs {
                enc.Close()
            }
            err = self.Storer.StoreBucket(self, tBuf, vBufs, tEnc.start, baseline, series)
            success <- err
        }
    }
}

func (self BucketStore) RollupAggregation(targetAggregation string) string {
    for _, a := range self.Aggregations {
        if a == targetAggregation {
            return a
        }
    }
    return self.Aggregations[0]
}

type timeEncoder struct {
    enc *bucket.BucketEncoder
    start time.Time
}

func newTimeEncoder(start time.Time, writer io.Writer) *timeEncoder {
    enc := &timeEncoder{
        enc: bucket.NewBucketEncoder(start.Unix(), writer),
        start: start,
    }
    return enc
}

func (self *timeEncoder) Write(t time.Time) {
    self.enc.WriteInt(t.Unix())
}

func (self *timeEncoder) Close() {
    self.enc.Close()
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

type block struct {
    tBytes, vBytes []byte
    start time.Time
    baseline, multiplier float64
}

func (self *block) Query(entries chan Entry, start time.Time, end time.Time) error {
    tBuf := make([]int64, 64)
    vBuf := make([]int64, 64)

    tDec := bucket.NewBucketDecoder(self.start.Unix(), bytes.NewBuffer(self.tBytes))
    vDec := bucket.NewBucketDecoder(int64(self.baseline * self.multiplier), bytes.NewBuffer(self.vBytes))

    for {
        tn, tErr := tDec.Read(tBuf)
        vn, vErr := vDec.Read(vBuf)
        if tn != vn {
            return errors.New("Mismatched number of times/values")
        }
        if tn > 0 {
            for i := 0; i < tn; i++ {
                ent := Entry{
                    Timestamp: time.Unix(tBuf[i], 0),
                    Value: float64(vBuf[i]) * (1.0 / self.multiplier),
                }
                entries <- ent
            }
        }
        if tn < len(tBuf) || vn < len(vBuf) {
            break
        }
        if tErr != nil {
            return tErr
        }
        if vErr != nil {
            return vErr
        }
    }
    return nil
}
