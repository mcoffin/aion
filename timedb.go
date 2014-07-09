package timedb

import (
    "bytes"
    "fmt"
    "time"
)

type InputPoint struct {
    Value float32
    Tags map[string]string
}

func (self *InputPoint) seriesID() string {
    var buffer bytes.Buffer
    for k, v := range self.Tags {
        buffer.WriteString(fmt.Sprintf("%s=%s|", k, v))
    }
    return buffer.String()
}

func (self *TimeDB) PutNow(point *InputPoint) error {
    t := time.Now()
    return self.Put(point, t)
}

func (self *TimeDB) Put(point *InputPoint, t time.Time) error {
    // TODO
    return nil
}
