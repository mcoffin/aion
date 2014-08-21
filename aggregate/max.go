package aggregate

import (
	"math"
	"sync"
	"time"
)

type MaxAggregator struct {
	first sync.Once
	max   float64
}

func (self *MaxAggregator) Add(value float64, timestamp time.Time) {
	self.first.Do(func() {
		self.max = value
	})
	self.max = math.Max(self.max, value)
}

func (self MaxAggregator) Value() float64 {
	return self.max
}

func (self *MaxAggregator) Reset() {
	self.first = sync.Once{}
}
