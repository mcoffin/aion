package aggregate

import (
	"sync"
	"time"
)

type MinAggregator struct {
	first sync.Once
	min   float64
}

func (self *MinAggregator) Add(value float64, timestamp time.Time) {
	self.first.Do(func() {
		self.min = value
	})
	if value < self.min {
		self.min = value
	}
}

func (self MinAggregator) Value() float64 {
	return self.min
}

func (self *MinAggregator) Reset() {
	self.first = sync.Once{}
}
