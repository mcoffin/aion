package timedb

import (
    "time"
)

type Cacher interface {
    cache(p *InputPoint,t time.Time) error
}
