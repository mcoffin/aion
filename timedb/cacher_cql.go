package timedb

import (
    "github.com/gocql/gocql"
)

type CQLCacher struct {
    *gocql.Session
}

func (self *CQLCacher) cache(p *InputPoint) error {
    return self.Query("insert into cache (series, time, value) values (?, ?, ?)", p.seriesID(), gocql.TimeUUID(), p.Value).Exec()
}
