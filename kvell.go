package kvell

// Store ...
type Store interface {
	Set(key string, value interface{}) error
	Get(key string, value interface{}) (found bool, err error)
	Delete(key string) error
	Close() error
}
