package aggregate

import (
    "fmt"
)

type Aggregator interface {
    Add(value float64)
    Value() float64
    Reset()
}

type RawAggregator struct {
    value float64
}

func (self *RawAggregator) Add(v float64) {
    self.value = v
}

func (self *RawAggregator) Value() float64 {
    return self.value
}

func (self *RawAggregator) Reset() {
    self.value = 0
}

func NewAggregator(aggregation string) (Aggregator, error) {
    switch aggregation {
    case "raw":
        return &RawAggregator{0}, nil
    default:
        return nil, fmt.Errorf("Can't find aggregator %s", aggregation)
    }
}
