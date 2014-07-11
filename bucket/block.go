package bucket

import (
    "bytes"
)

type bucket []byte

func (self bucket) decoder(base int64) *BucketDecoder {
    reader := bytes.NewBuffer(self)
    dec := NewBucketDecoder(base, reader)
    return dec
}

type Block struct {
    buckets []bucket
    Baseline int64
}

func NewBlock(base int64, bucketCount int) *Block {
    block := &Block{
        buckets: make([]bucket, bucketCount),
        Baseline: base,
    }
    return block
}

func (self *Block) WriteBucket(index int, values []int64) {
    buffer := &bytes.Buffer{}
    enc := NewBucketEncoder(self.Baseline, buffer)
    enc.Write(values)
    enc.Close()
    self.buckets[index] = buffer.Bytes()
}

func (self *Block) ReadBucket(index int, readBuffer []int64) []int64 {
    ret := make([]int64, 0)
    dec := self.buckets[index].decoder(self.Baseline)
    for {
        n, err := dec.Read(readBuffer)
        if n > 0 {
            ret = append(ret, readBuffer[:n]...)
        }
        if err != nil {
            return ret
        }
    }
}
