package timedb

import (
    "time"
)

func (self *TimeDB) Put(point *InputPoint) error {
    t := time.Now()
    return self.Cacher.cache(point, t)
}
