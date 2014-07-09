package timedb

import (
    "bytes"
    "math"
    "math/big"
    "time"
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

    // Multiplier for turning floats in to ints
    multiplier := Value(math.Pow10(int(precision)))

    // Round the baseline
    basev := int64(base.Value * multiplier)
    deltas := make([]int, len(self))
    var maxDelta int = 0

    //Store every delta, keeping track of the biggest one
    for i, entry := range self {
        next := int64(entry.Value * multiplier)
        deltas[i] = int(next - basev)
        basev = next
        dabs := deltas[i]
        if dabs < 0 {
            dabs = -dabs
        }
        if dabs > maxDelta {
            maxDelta = dabs
        }
    }

    // Find the size we'll need to store the deltas
    maxDeltaBig := big.NewInt(int64(maxDelta))
    deltaSize := byte(maxDeltaBig.BitLen())
    deltaSize++ // To store the sign

    // Write the delta size to the buffer
    buffer.WriteByte(deltaSize)

    return buffer.Bytes(), nil
}

type BlockData []BucketData

func (self BlockData) bucketBase() Value {
    // TODO
    return self[0][0].Value
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
