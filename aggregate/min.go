package aggregate

type MinAggregator struct {
    empty bool
    min float64
}

func (self *MinAggregator) Add(value float64) {
    if empty {
        min = value
        empty = false
    }
    if value < min {
        min = value
    }
}

func (self *MinAggregator) Value() {
    return min
}

func (self *MinAggregator) Reset() {
    min = 0
    empty = true
}
