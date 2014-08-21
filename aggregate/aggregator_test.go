package aggregate

import (
	"testing"
	"time"
)

var testVals = []float64{1.1, 2.2, -3.1}

func TestCountAggregator(t *testing.T) {
	a, err := NewAggregator("count")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for _, v := range testVals {
		a.Add(v, start)
		start = start.Add(time.Second)
	}
	realCount := float64(len(testVals))
	if a.Value() != realCount {
		t.Errorf("Expected count %v but got %v\n", realCount, a.Value())
	}
	a.Reset()
	if a.Value() != 0.0 {
		t.Errorf("Reset did not reset count to 0\n")
	}
}

func TestAvgAggregator(t *testing.T) {
	a, err := NewAggregator("avg")
	if err != nil {
		t.Fatal(err)
	}
	var sum float64 = 0.0
	start := time.Now()
	for _, v := range testVals {
		sum += v
		a.Add(v, start)
		start = start.Add(time.Second)
	}
	avg := (sum / float64(len(testVals)))
	val := a.Value()
	if val != avg {
		t.Errorf("Expected average %v but got %v\n", avg, val)
	}
	a.Reset()
	a.Add(testVals[0], time.Now())
	if a.Value() != testVals[0] {
		t.Errorf("Reset failed to reset avg\n")
	}
}

func TestMinAggregator(t *testing.T) {
	a, err := NewAggregator("min")
	if err != nil {
		t.Fatal(err)
	}
	min := testVals[0]
	start := time.Now()
	for _, v := range testVals {
		if v < min {
			min = v
		}
		a.Add(v, start)
		start = start.Add(time.Second)
	}
	val := a.Value()
	if val != min {
		t.Errorf("Expected min %v but got %v\n", min, val)
	}
	a.Reset()
	a.Add(testVals[0], time.Now())
	if a.Value() != testVals[0] {
		t.Errorf("Reset failed to reset min\n")
	}
}

func TestMaxAggregator(t *testing.T) {
	a, err := NewAggregator("max")
	if err != nil {
		t.Fatal(err)
	}
	min := testVals[0]
	start := time.Now()
	for _, v := range testVals {
		if v > min {
			min = v
		}
		a.Add(v, start)
		start = start.Add(time.Second)
	}
	val := a.Value()
	if val != min {
		t.Errorf("Expected max %v but got %v\n", min, val)
	}
	a.Reset()
	a.Add(testVals[0], time.Now())
	if a.Value() != testVals[0] {
		t.Errorf("Reset failed to reset max\n")
	}
}
