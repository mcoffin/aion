package delta

import (
    "math"
)

type Encoder struct {
    last int64
    multiplier float64
}

func NewEncoder(start float64, precision int) *Encoder {
    enc := &Encoder{
        multiplier: math.Pow10(precision),
    }
    enc.last = convertFloat64(start, enc.multiplier)
    return enc
}

func (self *Encoder) Encode(values []float64) []int {
    data := make([]int, len(values))
    for i, value := range values {
        data[i] = self.EncodeFloat64(value)
    }
    return data
}

func (self *Encoder) EncodeFloat64(value float64) int {
    next := convertFloat64(value, self.multiplier) // Use the global version to take advantage of inlining
    delta := int(next - self.last)
    self.last = next
    return delta
}

type Decoder struct {
    last float64
    multiplier float64
}

func NewDecoder(start float64, precision int) *Decoder {
    dec := &Decoder{
        multiplier: math.Pow10(-precision),
    }
    dec.last = start
    return dec
}

func (self *Decoder) Decode(values []int) []float64 {
    data := make([]float64, len(values))
    for i, value := range values {
        data[i] = self.DecodeFloat64(value)
    }
    return data
}

func (self *Decoder) DecodeFloat64(value int) float64 {
    delta := float64(value) * self.multiplier
    ret := self.last + delta
    self.last = ret
    return ret
}

func convertFloat64(value float64, multiplier float64) int64 {
    return int64(value * multiplier)
}
