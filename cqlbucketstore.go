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
            err = self.Session.Query("INSERT INTO data (series, duration, start, multiplier, baseline, times, values) VALUES (?, ?, ?, ?, ?, ?, ?)", seriesUUID, self.Duration / time.Second, enc.start, self.Multiplier, enc.baseline, tBuf.Bytes(), [][]byte{vBuf.Bytes()}).Exec()
            success <- err
            return
        }
    }
}

func (self *CQLBucketStore) bucketIndex(aggregation string) (int, error) {
    for i, a := range self.Aggregations {
        if a == aggregation {
            return i, nil
        }
    }
    return 0, errors.New("Can't find aggregation")
}

func (self *CQLBucketStore) Query(entries chan Entry, series uuid.UUID, aggregation string, start time.Time, end time.Time, success chan error) {
    seriesUUID, err := gocql.UUIDFromBytes(series)
    if err != nil {
        success <- err
        return
    }
    index, err := self.bucketIndex(aggregation)
    // If we don't have the aggregator, return an error
    if err != nil {
        success <- err
        return
    }
    var blk block
    var values [][]byte
    iter := self.Session.Query("SELECT times, values, start, baseline, multiplier FROM data WHERE series = ? AND duration = ? AND start >= ? AND start <= ?", seriesUUID, int(self.Duration.Seconds()), self.nearestStart(start), self.nearestStart(end)).Iter()
    for iter.Scan(&blk.tBytes, &values, &blk.start, &blk.baseline, &blk.multiplier) {
        blk.vBytes = values[index]
        err = blk.Query(entries, start, end)
        if err != nil {
            success <- err
            return
        }
    }
    err = iter.Close()
    close(entries)
    success <- err
}
