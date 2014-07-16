package timedb

import (
    "bytes"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
)

type CQLBucketStore struct {
    BucketStore
    Session *gocql.Session
}

func (self *CQLBucketStore) StoreBucket(store *BucketStore, times *bytes.Buffer, values []*bytes.Buffer, start time.Time, baseline float64, series uuid.UUID) error {
    seriesUUID, err := gocql.UUIDFromBytes(series)
    if err != nil {
        return err
    }
    valuesSlice := make([][]byte, len(values))
    for i, buf := range values {
        valuesSlice[i] = buf.Bytes()
    }

    return self.Session.Query("INSERT INTO data (series, duration, start, multiplier, baseline, times, values) VALUES (?, ?, ?, ?, ?, ?, ?)", seriesUUID, int(self.Duration.Seconds()), start, self.Multiplier, baseline, times.Bytes(), valuesSlice).Exec()
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
    iter := self.Session.Query("SELECT times, values, start, baseline, multiplier FROM data WHERE series = ? AND duration = ? AND start >= ? AND start <= ?", seriesUUID, int(self.Duration.Seconds()), start.Truncate(self.Duration), end.Truncate(self.Duration)).Iter()
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
