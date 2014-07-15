package aggregate

type MinAggregator struct {
    empty bool
    min float64
}

func (self *MinAggregator) Add(value float64) {
    if self.empty {
        self.min = value
        self.empty = false
    }
    if value < self.min {
        self.min = value
    }
}

func (self *MinAggregator) Value() float64 {
    return self.min
}

func (self *MinAggregator) Reset() {
    self.min = 0
    self.empty = true
}
