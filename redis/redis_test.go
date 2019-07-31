package redis

import (
	"testing"
	"time"
)

var option = Option{
	Host:     "localhost",
	Port:     6379,
	Password: "",
	TTL:      20 * time.Second,
}

// Close ...
func TestClose(t *testing.T) {
	client, err := NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSet(t *testing.T) {
	client, err := NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const (
		key   = "test"
		value = "testtesttest"
	)

	err = client.Set(key, value)
	if err != nil {
		t.Fatal(err)
	}
}

// Get ...
func TestGet(t *testing.T) {
	client, err := NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const (
		key = "test"
	)

	var value string
	_, err = client.Get(key, &value)
	if err != nil {
		t.Fatal(err)
	}
}

// Delete ...
func TestDelete(t *testing.T) {
	client, err := NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const (
		key = "test"
	)

	err = client.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSenario(t *testing.T) {
	client, err := NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const (
		key   = "test"
		value = "testtesttest"
	)

	err = client.Set(key, value)
	if err != nil {
		t.Fatal(err)
	}

	var result1 string
	found, err := client.Get(key, &result1)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("not found")
	}
	if result1 != value {
		t.Fatalf("different value")
	}

	err = client.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	var result2 string
	found, err = client.Get(key, &result2)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("why found?")
	}
}
