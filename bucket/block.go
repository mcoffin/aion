package bucket

import (
	"bytes"
)

type Block struct {
	buckets  []*bytes.Buffer
	Baseline int64
}

func NewBlock(base int64, bucketCount int) *Block {
	block := &Block{
		buckets:  make([]*bytes.Buffer, bucketCount),
		Baseline: base,
	}
	for i, _ := range block.buckets {
		block.buckets[i] = new(bytes.Buffer)
	}
	return block
}

func (self *Block) CreateBucketEncoder(index int) *BucketEncoder {
	return NewBucketEncoder(self.Baseline, self.buckets[index])
}

func (self *Block) CreateBucketDecoder(index int) *BucketDecoder {
	return NewBucketDecoder(self.Baseline, self.buckets[index])
}

func (self *Block) WriteBucket(index int, values []int64) {
	enc := self.CreateBucketEncoder(index)
	enc.Write(values)
	enc.Close()
}

func (self *Block) Bucket(index int) []byte {
	return self.buckets[index].Bytes()
}

func (self *Block) ReadBucket(index int, readBuffer []int64) []int64 {
	ret := make([]int64, 0)
	dec := self.CreateBucketDecoder(index)
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
