package timedb

import (
    "time"
)

type Entry struct {
    Value Value
    Time time.Time
}

type bucket []byte

type bucketizer interface {
    bucketize(base Entry) (bucket, error)
}

type BucketData []Entry

func (self BucketData) bucketize(base Entry) (bucket, error) {
    // TODO
    return nil, nil
}

type BlockData []BucketData

func (self BlockData) bucketBase() Value {
    // TODO
    return 0
}

func (self BlockData) Blockify(start time.Time) (Block, error) {
    base := Entry{
        Value: self.bucketBase(),
        Time: start,
    }
    block := Block{
        Baseline: base,
        buckets: make([]bucket, 0, len(self)),
    }
    for i, bker := range self {
        bk, err := bker.bucketize(block.Baseline)
        if err != nil {
            return block, err
        }
        block.buckets[i] = bk
    }
    return block, nil
}

type Blockifier interface {
    Blockify(start time.Time) (Block, error)
}

type Block struct {
    buckets []bucket
    Baseline Entry
}

func NewBlock(start time.Time, data Blockifier) (Block, error) {
    return data.Blockify(start)
}
