package bucket

import (
    "io"
    "code.google.com/p/deltagolomb"
)

type BucketEncoder struct {
    last int64
    genc *deltagolomb.ExpGolombEncoder
}

func NewBucketEncoder(start int64, out io.Writer) *BucketEncoder {
    enc := &BucketEncoder{
        last: start,
        genc: deltagolomb.NewExpGolombEncoder(out),
    }
    return enc
}

func (self *BucketEncoder) WriteInt(next int64) {
    self.genc.WriteInt(int(next - self.last))
    self.last = next
}

func (self *BucketEncoder) Write(values []int64) {
    for _, v := range values {
        self.WriteInt(v)
    }
}

func (self *BucketEncoder) Close() {
    self.genc.Close()
}

type BucketDecoder struct {
    last int64
    readBuf []int
    genc *deltagolomb.ExpGolombDecoder
}

func NewBucketDecoder(start int64, in io.Reader) *BucketDecoder {
    dec := &BucketDecoder{
        last: start,
        readBuf: make([]int, 1),
        genc: deltagolomb.NewExpGolombDecoder(in),
    }
    return dec
}

func (self *BucketDecoder) Read() (int64, error) {
    n, err := self.genc.Read(self.readBuf)
    if err != nil || n <= 0 {
        return 0, err
    }
    self.last += int64(self.readBuf[0])
    return self.last, nil
}
