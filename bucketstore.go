package timedb

import (
    "bytes"
    "errors"
    "io"
    "time"
    "github.com/FlukeNetworks/timedb/bucket"
)

// A representation of a backing store that keeps data in blocks and buckets
type BucketStore struct {
    Duration time.Duration
    Granularities []time.Duration
    Aggregations []string
}

// Rounds a time based on the duration of this BucketStore
func (self *BucketStore) nearestStart(t time.Time) time.Time {
    delta := t.Unix() % int64(self.Duration.Seconds())
    return time.Unix(t.Unix() - delta, 0)
}

type entryEncoder struct {
    tEnc, vEnc *bucket.BucketEncoder
    multiplier float64
    start time.Time
    baseline float64
}

func newEntryEncoder(start time.Time, baseline, multiplier float64, tWriter, vWriter io.Writer) *entryEncoder {
    enc := &entryEncoder{
        tEnc: bucket.NewBucketEncoder(start.Unix(), tWriter),
        vEnc: bucket.NewBucketEncoder(int64(baseline * multiplier), vWriter),
        multiplier: multiplier,
        start: start,
        baseline: baseline,
    }
    return enc
}

func (self *entryEncoder) Write(entry Entry) {
    self.tEnc.WriteInt(entry.Timestamp.Unix())
    self.vEnc.WriteInt(int64(entry.Value * self.multiplier))
}

func (self *entryEncoder) Close() {
    self.tEnc.Close()
    self.vEnc.Close()
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
