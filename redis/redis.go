package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/neilli-sable/kvell"
)

// Client ...
type Client struct {
	pool *redis.Pool
	ttl  time.Duration
}

// Option ...
type Option struct {
	Host     string
	Port     int
	Password string
	DB       int
	TTL      time.Duration
}

// NewClient ...
func NewClient(opt Option) (kvell.Store, error) {
	pool := newPool(fmt.Sprintf("%s:%d", opt.Host, opt.Port))
	return &Client{
		pool: pool,
		ttl:  opt.TTL,
	}, nil
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

// Set ...
func (c *Client) Set(key string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	args := []interface{}{key, data}
	if c.ttl.Nanoseconds() > 0 {
		args = []interface{}{key, data, "EX", c.ttl.Seconds()}
	}

	_, err = conn.Do("SET", args...)
	return err
}

// Get ...
func (c *Client) Get(key string, value interface{}) (found bool, err error) {
	conn := c.pool.Get()
	defer conn.Close()

	r, err := conn.Do("GET", key)
	if err != nil {
		return false, err
	}

	switch r := r.(type) {
	case []byte:
		err := json.Unmarshal(r, value)
		if err != nil {
			return false, err
		}
		return true, nil
	case string:
		err := json.Unmarshal([]byte(r), value)
		if err != nil {
			return false, err
		}
		return true, nil
	case nil:
		return false, nil
	case redis.Error:
		return false, r
	}

	return false, errors.New("redis return undefined value")
}

// UpdateTTL ...
func (c *Client) UpdateTTL(key string) error {
	conn := c.pool.Get()
	defer conn.Close()

	if c.ttl.Nanoseconds() == 0 {
		return nil
	}

	args := []interface{}{key, c.ttl.Seconds()}
	_, err := conn.Do("EXPIRE", args...)

	return err
}

// Delete ...
func (c *Client) Delete(key string) error {
	conn := c.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

// Close ...
func (c *Client) Close() error {
	return c.pool.Close()
}
