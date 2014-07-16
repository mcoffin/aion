package aggregate

type AvgAggregator struct {
    count int
    sum float64
}

func (self *AvgAggregator) Reset() {
    self.count = 0
    self.sum = 0.0
}

func (self *AvgAggregator) Add(value float64) {
    self.sum += value
    self.count++
}

func (self AvgAggregator) Value() float64 {
    return self.sum / float64(self.count)
}
