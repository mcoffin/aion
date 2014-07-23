package aggregate

import "math"

type MaxAggregator struct {
	first bool
	max   float64
}

func (self *MaxAggregator) Add(value float64) {
	if self.first {
		self.max = value
		self.first = false
		return
	}
	self.max = math.Max(self.max, value)
}

func (self MaxAggregator) Value() float64 {
	return self.max
}

func (self *MaxAggregator) Reset() {
	self.first = true
}
