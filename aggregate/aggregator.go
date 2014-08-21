package aggregate

import (
	"fmt"
	"time"
)

type Aggregator interface {
	Add(value float64, timestamp time.Time)
	Value() float64
	Reset()
}

type RawAggregator struct {
	value float64
}

func (self *RawAggregator) Add(v float64, timestamp time.Time) {
	self.value = v
}

func (self RawAggregator) Value() float64 {
	return self.value
}

func (self *RawAggregator) Reset() {
	self.value = 0
}

type CountAggregator struct {
	count int
}

func (self *CountAggregator) Add(v float64, timestamp time.Time) {
	self.count++
}

func (self CountAggregator) Value() float64 {
	return float64(self.count)
}

func (self *CountAggregator) Reset() {
	self.count = 0
}

func NewAggregator(aggregation string) (Aggregator, error) {
	var a Aggregator
	switch aggregation {
	case "raw":
		a = new(RawAggregator)
	case "min":
		a = new(MinAggregator)
	case "tmin":
		a = new(TMinAggregator)
	case "max":
		a = new(MaxAggregator)
	case "tmax":
		a = new(TMaxAggregator)
	case "avg":
		a = new(AvgAggregator)
	case "count":
		a = new(CountAggregator)
	default:
		return nil, fmt.Errorf("Can't find aggregator %s", aggregation)
	}
	a.Reset()
	return a, nil
}
