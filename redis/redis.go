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
	conn redis.Conn
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
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", opt.Host, opt.Port))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		ttl:  opt.TTL,
	}, nil
}

// Set ...
func (c *Client) Set(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	args := []interface{}{key, data}
	if c.ttl.Nanoseconds() > 0 {
		args = []interface{}{key, data, "EX", c.ttl.Seconds()}
	}

	_, err = c.conn.Do("SET", args...)
	return err
}

// Get ...
func (c *Client) Get(key string, value interface{}) (found bool, err error) {
	r, err := c.conn.Do("GET", key)
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

// Delete ...
func (c *Client) Delete(key string) error {
	_, err := c.conn.Do("DEL", key)
	return err
}

// Close ...
func (c *Client) Close() error {
	return c.conn.Close()
}
