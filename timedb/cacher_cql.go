package timedb

import (
    "time"
    "github.com/gocql/gocql"
)

type CQLCacher struct {
    *gocql.Session
}

func (self *CQLCacher) cache(p *InputPoint, t time.Time) error {
    return self.Query("insert into cache (series, time, value) values (?, ?, ?)", p.seriesID(), gocql.UUIDFromTime(t), p.Value).Exec()
}
