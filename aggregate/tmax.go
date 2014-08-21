package aggregate

import (
	"sync"
	"time"
)

type TMaxAggregator struct {
	first sync.Once
	max   float64
	tMax  time.Time
}

func (self *TMaxAggregator) Add(value float64, timestamp time.Time) {
	self.first.Do(func() {
		self.max = value
		self.tMax = timestamp
	})
	if value > self.max {
		self.max = value
		self.tMax = timestamp
	}
}

func (self TMaxAggregator) Value() float64 {
	return float64(self.tMax.Unix())
}

func (self *TMaxAggregator) Reset() {
	self.first = sync.Once{}
}
