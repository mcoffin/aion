package timedb

import (
    "bytes"
    "encoding/binary"
    "fmt"
    "time"
    "code.google.com/p/go-uuid/uuid"
    "launchpad.net/goamz/s3"
)

type S3BucketStore struct {
    BucketStore
    Bucket *s3.Bucket
}

func (self S3BucketStore) s3BlockPath(series uuid.UUID, start time.Time) string {
    return fmt.Sprintf("%s/%d/%d", series.String(), int(self.Duration.Seconds()), start.Unix())
}

type s3BucketHeader struct {
    Multiplier float64
    Baseline float64
    Aggregators uint16
}

func (self *S3BucketStore) StoreBucket(store *BucketStore, times *bytes.Buffer, values []*bytes.Buffer, start time.Time, baseline float64, series uuid.UUID) error {
    fileBuf := new(bytes.Buffer)
    header := s3BucketHeader{
        Multiplier: self.Multiplier,
        Baseline: baseline,
        Aggregators: uint16(len(values)),
    }
    err := binary.Write(fileBuf, binary.LittleEndian, &header)
    if err != nil {
        return err
    }
    err = binary.Write(fileBuf, binary.LittleEndian, uint16(times.Len()))
    if err != nil {
        return err
    }
    for _, buf := range values {
        err = binary.Write(fileBuf, binary.LittleEndian, uint16(buf.Len()))
        if err != nil {
            return err
        }
    }
    fileBuf.Write(times.Bytes())
    for _, buf := range values {
        fileBuf.Write(buf.Bytes())
    }
    err = self.Bucket.Put(self.s3BlockPath(series, start.Truncate(self.Duration)), fileBuf.Bytes(), "application/octet-stream", s3.BucketOwnerFull)
    return err
}

func (self *S3BucketStore) Query(entries chan Entry, series uuid.UUID, aggregation string, start time.Time, end time.Time, success chan error) {
    for currentTime := start.Truncate(self.Duration); !currentTime.After(end) && !currentTime.Equal(end); currentTime = currentTime.Add(self.Duration) {
        bytes, err := self.Bucket.Get(self.s3BlockPath(series, currentTime))
        if err != nil {
            success <- err
            return
        }
        err = self.queryBucket(entries, bytes, aggregation, currentTime, end)
        if err != nil {
            success <- err
            return
        }
    }
    close(entries)
    success <- nil
}

func (self *S3BucketStore) queryBucket(entries chan Entry, data []byte, aggregation string, start time.Time, end time.Time) error {
    reader := bytes.NewBuffer(data)
    var header s3BucketHeader
    var tLen uint16
    err := binary.Read(reader, binary.LittleEndian, &header)
    if err != nil {
        return err
    }
    err = binary.Read(reader, binary.LittleEndian, &tLen)
    vBufs := make([][]byte, header.Aggregators)
    for i, _ := range vBufs {
        var vLen uint16
        err = binary.Read(reader, binary.LittleEndian, &vLen)
        if err != nil {
            return err
        }
        vBufs[i] = make([]byte, vLen)
    }
    tBuf := make([]byte, tLen)
    n, err := reader.Read(tBuf)
    if n != int(tLen) {
        fmt.Errorf("expected bucket of length %d but got %d bytes", tLen, n)
    }
    for _, buf := range vBufs {
        n, err = reader.Read(buf)
        if n != len(buf) {
            fmt.Errorf("Expected bucket of length %d but got %d bytes", len(buf), n)
        }
    }
    index, err := self.bucketIndex(aggregation)
    if err != nil {
        return err
    }
    block := &block{
        tBytes: tBuf,
        vBytes: vBufs[index],
        start: start,
        baseline: header.Baseline,
        multiplier: header.Multiplier,
    }
    return block.Query(entries, start, end)
}
