package kvell

// Store ...
type Store interface {
	Health() error
	Set(key string, value interface{}) error
	Get(key string, value interface{}) (found bool, err error)
	UpdateTTL(key string) error
	Delete(key string) error
	Close() error
}
