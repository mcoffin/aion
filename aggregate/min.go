package aggregate

type MinAggregator struct {
	first bool
	min   float64
}

func (self *MinAggregator) Add(value float64) {
	if self.first {
		self.min = value
		self.first = false
		return
	}
	if value < self.min {
		self.min = value
	}
}

func (self MinAggregator) Value() float64 {
	return self.min
}

func (self *MinAggregator) Reset() {
	self.first = true
}
