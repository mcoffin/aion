package timedb

func (self *TimeDB) Put(point *InputPoint) error {
    return self.Cacher.cache(point)
}
