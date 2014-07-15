package timedb

import (
    "bytes"
    "errors"
    "io"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
    "github.com/FlukeNetworks/timedb/bucket"
)

type CQLBucketStore struct {
    BucketStore
    Session *gocql.Session
    Multiplier float64
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

func (self *CQLBucketStore) Insert(entries chan Entry, series uuid.UUID, success chan error) {
    seriesUUID, err := gocql.UUIDFromBytes(series)
    var enc *entryEncoder
    tBuf := &bytes.Buffer{}
    vBuf := &bytes.Buffer{}
    if err != nil {
        success <- err
        return
    }
    for {
        entry, more := <-entries
        if more {
            if enc == nil {
                enc = newEntryEncoder(entry.Timestamp, entry.Value, self.Multiplier, tBuf, vBuf)
            }
            enc.Write(entry)
        } else {
            enc.Close()
            err = self.Session.Query("INSERT INTO data (series, duration, start, accuracy, baseline, buckets) VALUES (?, ?, ?, ?, ?, ?)", seriesUUID, self.Duration / time.Second, enc.start, self.Multiplier, enc.baseline, [][]byte{tBuf.Bytes(), vBuf.Bytes()}).Exec()
            success <- err
            return
        }
    }
}

func (self *CQLBucketStore) bucketIndices(granularity time.Duration, aggregation string) (int, int, error) {
    var tIndex, vIndex int
    found := false
    for i, g := range self.Granularities {
        if g == granularity {
            tIndex = i
            found = true
        }
    }
    if !found {
        return tIndex, vIndex, errors.New("Invalid granularity")
    }
    found = false
    for i, a := range self.Aggregations {
        if a == aggregation {
            vIndex = (tIndex * len(self.Aggregations)) + i
            found = true
        }
    }
    if !found {
        return tIndex, vIndex, errors.New("Invalid aggregation")
    }
    return tIndex, vIndex, nil
}

type block struct {
    tBytes, vBytes []byte
    start time.Time
    baseline, multiplier float64
}

func (self *block) Query(entries chan Entry, start time.Time, end time.Time) error {
    tBuf := make([]int64, len(entries))
    vBuf := make([]int64, len(entries))

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
                entries <- Entry{
                    Timestamp: time.Unix(tBuf[i], 0),
                    Value: float64(vBuf[i]) * self.multiplier,
                }
            }
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

func (self *CQLBucketStore) Query(entries chan Entry, series uuid.UUID, granularity time.Duration, aggregation string, start time.Time, end time.Time, success chan error) {
    seriesUUID, err := gocql.UUIDFromBytes(series)
    if err != nil {
        success <- err
        return
    }
    tIndex, vIndex, err := self.bucketIndices(granularity, aggregation)
    var blk block
    // If we can't find the right bucket, give raw data
    if err != nil {
        iter := self.Session.Query("SELECT time_raw, value_raw, start, baseline, multiplier FROM data WHERE series = ? AND duration = ? AND start >= ? AND start <= ?", seriesUUID, int(self.Duration.Seconds()), self.nearestStart(start), self.nearestStart(end)).Iter()
        for iter.Scan(&blk.tBytes, &blk.vBytes, &blk.start, &blk.baseline, &blk.multiplier) {
            err = blk.Query(entries, start, end)
            if err != nil {
                success <- err
                return
            }
        }
        if err = iter.Close(); err != nil {
            success <- err
            return
        }
    } else {
        var timeBuckets, valueBuckets [][]byte
        iter := self.Session.Query("SELECT time_aggregated, value_aggregated, start, baseline, multiplier FROM data WHERE series = ? AND duration = ? AND start >= ? AND start <= ?", seriesUUID, int(self.Duration.Seconds()), self.nearestStart(start), self.nearestStart(end)).Iter()
        for iter.Scan(&timeBuckets, &valueBuckets, &blk.start, &blk.baseline, &blk.multiplier) {
            blk.tBytes = timeBuckets[tIndex]
            blk.vBytes = valueBuckets[vIndex]
            err = blk.Query(entries, start, end)
            if err != nil {
                success <- err
                return
            }
        }
        if err = iter.Close(); err != nil {
            success <- err
            return
        }
    }
}
