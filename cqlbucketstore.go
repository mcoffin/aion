package timedb

import (
    "bytes"
    "errors"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
)

type CQLBucketStore struct {
    BucketStore
    Session *gocql.Session
    Multiplier float64
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
                enc = newEntryEncoder(self.nearestStart(entry.Timestamp), entry.Value, self.Multiplier, tBuf, vBuf)
            }
            enc.Write(entry)
        } else {
            enc.Close()
            err = self.Session.Query("INSERT INTO data (series, duration, start, multiplier, baseline, time_raw, value_raw) VALUES (?, ?, ?, ?, ?, ?, ?)", seriesUUID, self.Duration / time.Second, enc.start, self.Multiplier, enc.baseline, tBuf.Bytes(), vBuf.Bytes()).Exec()
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
        err = iter.Close()
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
        err = iter.Close()
    }
    close(entries)
    success <- err
}
