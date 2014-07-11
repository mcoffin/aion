package timedb

import (
    "bytes"
    "io"
    "math"
    "time"
    "github.com/FlukeNetworks/timedb/bucket"
    "fmt"
)

type Entry struct {
    Value float64
    Time time.Time
}

type blockData [][]Entry

func (self blockData) blockBase() float64 {
    sum := float64(0.0)
    for _, entries := range self {
        sum += entries[0].Value
    }
    return sum / float64(len(self))
}

type Block struct {
    buckets [][]byte
    Start time.Time
    Baseline float64
    Precision int
}

func NewBlock(start time.Time, precision int, values [][]Entry) *Block {
    data := blockData(values)
    block := &Block{
        buckets: make([][]byte, len(values) * 2),
        Baseline: data.blockBase(),
        Start: start,
        Precision: precision,
    }
    for i, entries := range values {
        tBuffer := &bytes.Buffer{}
        vBuffer := &bytes.Buffer{}
        enc := block.createBlockEncoder(vBuffer, tBuffer)
        for _, entry := range entries {
            enc.Write(&entry)
        }
        enc.Close()
        index := 2 * i
        block.buckets[index] = tBuffer.Bytes()
        block.buckets[index + 1] = vBuffer.Bytes()
    }
    return block
}

func (self *Block) createBlockEncoder(valueWriter, timeWriter io.Writer) *blockEncoder {
    enc := &blockEncoder{
        multiplier: math.Pow10(self.Precision),
        tEnc: bucket.NewBucketEncoder(self.Start.Unix(), timeWriter),
    }
    enc.vEnc = bucket.NewBucketEncoder(int64(self.Baseline * enc.multiplier), valueWriter)
    return enc
}

func (self *Block) createBucketDecoder(index int) *bucket.BucketDecoder {
    buffer := bytes.NewBuffer(self.buckets[index])
    var start int64
    if index % 2 == 0 {
        start = self.Start.Unix()
    } else {
        start = int64(self.Baseline * math.Pow10(self.Precision))
    }
    return bucket.NewBucketDecoder(start, buffer)
}

func (self *Block) CreateBlockDecoder(index int) *BlockDecoder {
    index *= 2
    dec := &BlockDecoder{
        multiplier: math.Pow10(-self.Precision),
        tDec: self.createBucketDecoder(index),
        vDec: self.createBucketDecoder(index + 1),
    }
    fmt.Printf("Time Bucket: %d\nValue Bucket: %d\n", index, index + 1)
    return dec
}

type blockEncoder struct {
    multiplier float64
    vEnc, tEnc *bucket.BucketEncoder
}

func (self *blockEncoder) writeValue(value float64) {
    v := int64(value * self.multiplier)
    fmt.Printf("Writing value %d for %f\n", v, value)
    self.vEnc.WriteInt(v)
}

func (self *blockEncoder) writeTime(t time.Time) {
    v := t.Unix()
    fmt.Printf("Writing time %d\n", v)
    self.tEnc.WriteInt(v)
}

func (self *blockEncoder) Write(entry *Entry) {
    self.writeValue(entry.Value)
    self.writeTime(entry.Time)
}

func (self *blockEncoder) Close() {
    self.vEnc.Close()
    self.tEnc.Close()
}

type BlockDecoder struct {
    multiplier float64
    vDec, tDec *bucket.BucketDecoder
}

func (self *BlockDecoder) readValue() (float64, error) {
    v, err := self.vDec.Read()
    if err != nil {
        return 0, err
    }
    return (float64(v) * self.multiplier), nil
}

func (self *BlockDecoder) readTime() (time.Time, error) {
    v, err := self.vDec.Read()
    if err != nil {
        return time.Now(), err
    }
    return time.Unix(v, 0), nil
}

func (self *BlockDecoder) Read() (Entry, error) {
    value, err := self.readValue()
    if err != nil {
        return Entry{}, err
    }
    t, err := self.readTime()
    if err != nil {
        return Entry{}, err
    }
    return Entry{
        Value: value,
        Time: t,
    }, nil
}
