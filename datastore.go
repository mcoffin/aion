package timedb

type Storer interface {
    Store(series string,level QueryLevel, block *Block) error
}
