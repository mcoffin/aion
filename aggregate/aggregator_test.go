package aggregate

import (
	"testing"
)

var testVals = []float64{1.1, 2.2, -3.1}

func TestAvgAggregator(t *testing.T) {
	a, err := NewAggregator("avg")
	if err != nil {
		t.Fatal(err)
	}
	var sum float64 = 0.0
	for _, v := range testVals {
		sum += v
		a.Add(v)
	}
	avg := (sum / float64(len(testVals)))
	val := a.Value()
	if val != avg {
		t.Fatalf("Expected average %v but got %v\n", avg, val)
	}
}

func TestMinAggregator(t *testing.T) {
	a, err := NewAggregator("min")
	if err != nil {
		t.Fatal(err)
	}
	min := testVals[0]
	for _, v := range testVals {
		if v < min {
			min = v
		}
		a.Add(min)
	}
	val := a.Value()
	if val != min {
		t.Fatalf("Expected min %v but got %v\n", min, val)
	}
}

func TestMaxAggregator(t *testing.T) {
	a, err := NewAggregator("max")
	if err != nil {
		t.Fatal(err)
	}
	min := testVals[0]
	for _, v := range testVals {
		if v > min {
			min = v
		}
		a.Add(min)
	}
	val := a.Value()
	if val != min {
		t.Fatalf("Expected max %v but got %v\n", min, val)
	}
}
