package timedb

import (
    "bytes"
    "io"
    "time"
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
        buckets: make([][]byte, len(values)),
        Baseline: data.blockBase(),
        Start: start,
        Precision: precision,
    }
    for i, entries := range values {
        buffer := &bytes.Buffer{}
        enc := block.createBucketEncoder(buffer)
        for _, entry := range entries {
            enc.WriteFloat64(entry.Value)
        }
        enc.Close()
        block.buckets[i] = make([]byte, len(buffer.Bytes()))
        copy(block.buckets[i], buffer.Bytes())
    }
    return block
}

func (self *Block) CreateBucketDecoder(index int) *BucketDecoder {
    buffer := bytes.NewBuffer(self.buckets[index])
    if index % 2 == 0 {
        return NewBucketDecoder(self.Baseline, self.Precision, buffer)
    } else {
        return NewTimeBucketDecoder(self.Start, buffer)
    }
}

func (self *Block) createBucketEncoder(out io.Writer) *bucketEncoder {
    return newBucketEncoder(self.Baseline, self.Precision, out)
}

type BlockDecoder struct {
    vDec *BucketDecoder
    tDec *BucketDecoder
}

func (self *Block) CreateBlockDecoder(index int) *BlockDecoder {
    index *= 2
    dec := &BlockDecoder{
        vDec: self.CreateBucketDecoder(index),
        tDec: self.CreateBucketDecoder(index+1),
    }
    return dec
}

func (self *BlockDecoder) ReadEntry() (Entry, error) {
    v, err := self.vDec.ReadFloat64()
    if err != nil {
        return Entry{}, err
    }
    t, err := self.tDec.ReadTime()
    if err != nil {
        return Entry{}, err
    }
    return Entry{
        Value: v,
        Time: t,
    }, nil
}
