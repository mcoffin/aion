package timedb

type Cacher interface {
    cache(p *InputPoint) error
}
