package timedb

import (
    "time"
    "code.google.com/p/go-uuid/uuid"
)

const (
    channelSize = 4
)

// One entry in a time series
type Entry struct {
    Timestamp time.Time
    Value float64
}

type InputPoint struct {
    Entry Entry
    Series uuid.UUID
    Aggregator string
}

type QueryLevel interface {
    Insert(series uuid.UUID, entry Entry, aggregator string) error
}

type levelContext struct {
    pointsChannel chan InputPoint
    errorsChannel chan error
}

type TimeDB struct {
    QueryLevels []QueryLevel
    contexts []levelContext
}

func NewTimeDB(qLevels ...QueryLevel) *TimeDB {
    db := &TimeDB{
        QueryLevels: qLevels,
        contexts: make([]levelContext, len(qLevels)),
    }
    return db
}

func (self *TimeDB) Start() {
    for i, level := range self.QueryLevels {
        self.contexts[i] = levelContext{
            pointsChannel: make(chan InputPoint, channelSize),
            errorsChannel: make(chan error, channelSize),
        }
        go Inserts(level, self.contexts[i].pointsChannel, self.contexts[i].errorsChannel)
    }
}

func (self *TimeDB) Put(series uuid.UUID, entry Entry) error {
    point := InputPoint{entry, series, "raw"}
    for _, context := range self.contexts {
        context.pointsChannel <- point
    }
    var err error
    for _, context := range self.contexts {
        err = <-context.errorsChannel
    }
    return err
}

func Inserts(level QueryLevel, points chan InputPoint, errors chan error) {
    for {
        point, more := <-points
        if !more {
            break
        }
        errors <- level.Insert(point.Series, point.Entry, point.Aggregator)
    }
}
