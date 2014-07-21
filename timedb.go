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
    Series uuid.UUID
    Timestamp time.Time
    Value float64
}

type QueryLevel interface {
    Insert(entries chan Entry, errors chan error)
}

type TimeDB struct {
    QueryLevels []QueryLevel
    entryChannels []chan Entry
    errorChannels []chan error
}

func NewTimeDB(qLevels ...QueryLevel) *TimeDB {
    db := &TimeDB{
        QueryLevels: qLevels,
        entryChannels: make([]chan Entry, len(qLevels),
        errorChannels: make([]chan error, len(qLevels),
    }
    return db
}

func (self *TimeDB) Start() {
    for i, level := range self.QueryLevels {
        self.entryChannels[i] = make(chan Entry, channelSize)
        self.errorChannels[i] = make(chan error)
        go level.Insert(self.entryChannels[i], self.errorChannels[i])
    }
}

func (self *TimeDB) Put(entry Entry) error {
    for _, entries := range self.entryChannels {
        entries <- entry
    }
    for _, errors := range self.errorChannels {
        err := <-errors
        if err != nil {
            return err
        }
    }
    return nil
}
