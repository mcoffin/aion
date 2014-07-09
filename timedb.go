package timedb

import (
    "bytes"
    "fmt"
    "time"
)

type Value float32

type TimeDB struct {
    Levels []QueryLevel
}

func NewTimeDB(customLevels ...QueryLevel) TimeDB {
    cacheQueryLevel := QueryLevel{
        Duration: time.Second,
        Aggregations: []AggregationLevel{},
    }
    tdb := TimeDB{
        Levels: make([]QueryLevel, 0, len(customLevels) + 1),
    }

    tdb.Levels[0] = cacheQueryLevel
    for i, level := range customLevels {
        tdb.Levels[i+1] = level
    }

    return tdb
}

func (self *TimeDB) PutNow(point *InputPoint) error {
    t := time.Now()
    return self.Put(point, t)
}

func (self *TimeDB) Put(point *InputPoint, t time.Time) error {
    // TODO
    return nil
}

type InputPoint struct {
    Value Value
    Tags map[string]string
}

func (self *InputPoint) seriesID() string {
    var buffer bytes.Buffer
    for k, v := range self.Tags {
        buffer.WriteString(fmt.Sprintf("%s=%s|", k, v))
    }
    return buffer.String()
}
