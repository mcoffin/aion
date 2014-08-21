package aggregate

import (
	"sync"
	"time"
)

type TMinAggregator struct {
	first sync.Once
	min   float64
	tMin  time.Time
}

func (self *TMinAggregator) Add(value float64, timestamp time.Time) {
	self.first.Do(func() {
		self.min = value
		self.tMin = timestamp
	})
	if value < self.min {
		self.min = value
		self.tMin = timestamp
	}
}

func (self TMinAggregator) Value() float64 {
	return float64(self.tMin.Unix())
}

func (self *TMinAggregator) Reset() {
	self.first = sync.Once{}
}
