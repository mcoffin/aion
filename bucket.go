package timedb

import (
    "io"
    "time"
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

func newTimeBucketEncoder(base time.Time, out io.Writer) *bucketEncoder {
    enc := &bucketEncoder{
        denc: delta.NewTimeEncoder(base),
        genc: deltagolomb.NewExpGolombEncoder(out),
    }
    return enc
}

func (self *bucketEncoder) WriteTime(value time.Time) {
    delta := self.denc.EncodeTime(value)
    self.genc.WriteInt(delta)
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

type bucketDecoder struct {
    ddec *delta.Decoder
    gdec *deltagolomb.ExpGolombDecoder
}

func newBucketDecoder(base float64, precision int, in io.Reader) *bucketDecoder {
    dec := &bucketDecoder{
        ddec: delta.NewDecoder(base, precision),
        gdec: deltagolomb.NewExpGolombDecoder(in),
    }
    return dec
}
