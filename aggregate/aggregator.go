package aggregate

type Aggregator interface {
    Add(value float64)
    Value() float64
    Reset()
}
