package timedb

import (
    "bytes"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "github.com/gocql/gocql"
    "github.com/FlukeNetworks/timedb/bucket"
)

type CQLBucketStore struct {
    Duration time.Duration
    Session *gocql.Session
    Multiplier float64
    Granularities []time.Duration
}

func (self *CQLBucketStore) Insert(entries chan Entry, series uuid.UUID, success chan error) {
    seriesUUID, err := gocql.UUIDFromBytes(series)
    var tEnc, vEnc *bucket.BucketEncoder
    var tStart time.Time
    var vStart float64
    tBuf := &bytes.Buffer{}
    vBuf := &bytes.Buffer{}
    if err != nil {
        success <- err
        return
    }
    for {
        entry, more := <-entries
        if more {
            if tEnc == nil {
                tEnc = bucket.NewBucketEncoder(entry.Timestamp.Unix(), tBuf)
                tStart = entry.Timestamp
            }
            if vEnc == nil {
                vEnc = bucket.NewBucketEncoder(int64(entry.Value * self.Multiplier), vBuf)
                vStart = entry.Value
            }
            tEnc.WriteInt(entry.Timestamp.Unix())
            vEnc.WriteInt(int64(entry.Value * self.Multiplier))
        } else {
            tEnc.Close()
            vEnc.Close()
            err = self.Session.Query("INSERT INTO data (series, duration, start, accuracy, baseline, buckets) VALUES (?, ?, ?, ?, ?, ?)", seriesUUID, self.Duration / time.Second, tStart, self.Multiplier, vStart, [][]byte{tBuf.Bytes(), vBuf.Bytes()}).Exec()
            success <- err
            return
        }
    }
}
