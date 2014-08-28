package bucket

import (
	"io"

	"bitbucket.org/m_coffin/deltagolomb"
)

// A BucketEncoder writes a series of integers to a writer
type BucketEncoder struct {
	last int64
	genc *deltagolomb.ExpGolombEncoder
}

// Creates a new BucketEncoder with a context around the given start value
// that will write to `out`
func NewBucketEncoder(start int64, out io.Writer) *BucketEncoder {
	enc := &BucketEncoder{
		last: start,
		genc: deltagolomb.NewExpGolombEncoder(out),
	}
	return enc
}

// Writes an integer from a series to the BucketEncoder
func (self *BucketEncoder) WriteInt(next int64) {
	self.genc.WriteInt(int(next - self.last))
	self.last = next
}

// Convenience method for batch-writing values
func (self *BucketEncoder) Write(values []int64) {
	for _, v := range values {
		self.WriteInt(v)
	}
}

// "Flushes" any remaining partial bits to w
func (self *BucketEncoder) Flush(w io.Writer) {
	self.genc.WritePartialBits(w)
}

// "Closes" the encoder, flushing all un-written values.
func (self *BucketEncoder) Close() {
	self.genc.Close()
}

// A BucketDecoder reads a delta-encoded stream of integers
type BucketDecoder struct {
	last int64
	genc *deltagolomb.ExpGolombDecoder
}

// Creates a new BucketDecoder with a context around the given start point
// that will read its encoded data from `in`
func NewBucketDecoder(start int64, in io.Reader) *BucketDecoder {
	dec := &BucketDecoder{
		last: start,
		genc: deltagolomb.NewExpGolombDecoder(in),
	}
	return dec
}

// Reads a bunch of encoded integers into a buffer
func (self *BucketDecoder) Read(out []int64) (int, error) {
	deltas := make([]int, len(out))
	n, err := self.genc.Read(deltas)
	for i, delta := range deltas[:n] {
		self.last += int64(delta)
		out[i] = self.last
	}
	return n, err
}
