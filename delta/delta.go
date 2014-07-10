package delta

import (
    "math"
    "time"
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

func NewTimeEncoder(start time.Time) *Encoder {
    enc := &Encoder{
        multiplier: 0.0,
    }
    enc.last = start.Unix()
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

func (self *Encoder) EncodeTime(value time.Time) int {
    next := value.Unix()
    delta := int(next - self.last)
    self.last = next
    return delta
}

type Decoder struct {
    last int64
    multiplier float64
}

func NewDecoder(start float64, precision int) *Decoder {
    dec := &Decoder{
        multiplier: math.Pow10(-precision),
    }
    dec.last = convertFloat64(start, math.Pow10(precision))
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
    ret := self.last + int64(value)
    self.last = ret
    return reverseFloat64(ret, self.multiplier)
}

func (self *Decoder) DecodeTime(value int) time.Time {
    ret := self.last + int64(value)
    self.last = ret
    return time.Unix(ret, 0)
}

func reverseFloat64(value int64, multiplier float64) float64 {
    return float64(value) * multiplier
}

func convertFloat64(value float64, multiplier float64) int64 {
    return int64(value * multiplier)
}
