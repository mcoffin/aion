package timedb

import (
    "bytes"
    "math"
    "time"
    "code.google.com/p/deltagolomb"
)

type Entry struct {
    Value Value
    Time time.Time
}

type bucket []byte

type bucketizer interface {
    bucketize(base Entry, precision uint) (bucket, error)
}

type BucketData []Entry

func (self BucketData) bucketize(base Entry, precision uint) (bucket, error) {
    buffer := bytes.Buffer{}
    enc := deltagolomb.NewExpGolombEncoder(&buffer)

    multiplier := Value(math.Pow10(int(precision)))
    last := int64(base.Value * multiplier)
    for _, entry := range self {
        v := int64(entry.Value * multiplier)
        delta := int(v - last)
        last = v
        enc.Write([]int{delta})
    }
    enc.Close()

    return buffer.Bytes(), nil
}

func (self BucketData) bucketBase() Value {
    sum := Value(0)
    for _, entry := range self {
        sum += entry.Value
    }
    return sum / Value(len(self))
}

type BlockData []BucketData

func (self BlockData) bucketBase() Value {
    sum := Value(0)
    for _, bd := range self {
        sum += bd.bucketBase()
    }
    return sum / Value(len(self))
}

func (self BlockData) Blockify(start time.Time, precision uint) (Block, error) {
    base := Entry{
        Value: self.bucketBase(),
        Time: start,
    }
    block := Block{
        Baseline: base,
        buckets: make([]bucket, len(self)),
    }
    for i, bker := range self {
        bk, err := bker.bucketize(block.Baseline, precision)
        if err != nil {
            return block, err
        }
        block.buckets[i] = bk
    }
    return block, nil
}

type Blockifier interface {
    Blockify(start time.Time, precision uint) (Block, error)
}

type Block struct {
    buckets []bucket
    Baseline Entry
}

func NewBlock(start time.Time, data Blockifier, precision uint) (Block, error) {
    return data.Blockify(start, precision)
}
