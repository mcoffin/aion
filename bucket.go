package timedb

import (
    "io"
    "github.com/FlukeNetworks/timedb/delta"
    "code.google.com/p/deltagolomb"
)

type bucketEncoder struct {
    denc *delta.Encoder
    genc *deltagolomb.ExpGolombEncoder
}

func newBucketEncoder(base float64, precision int, out io.Writer) *bucketEncoder {
    enc := &bucketEncoder{
        denc: delta.NewEncoder(base, precision),
        genc: deltagolomb.NewExpGolombEncoder(out),
    }
    return enc
}

func (self *bucketEncoder) WriteFloat64(value float64) {
    delta := self.denc.EncodeFloat64(value)
    self.genc.WriteInt(delta)
}

func (self *bucketEncoder) Write(values []float64) {
    deltas := self.denc.Encode(values)
    self.genc.Write(deltas)
}

func (self *bucketEncoder) Close() {
    self.genc.Close()
}
