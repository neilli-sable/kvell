package dynamodb

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var option = Option{
	Region:               "us-west-2",
	TableName:            "test",
	ReadCapacityUnits:    1,
	WriteCapacityUnits:   1,
	WaitForTableCreation: aws.Bool(true),
	AWSaccessKeyID:       "dummy",
	AWSsecretAccessKey:   "fake",
	CustomEndpoint:       "http://localhost:8000",
	TTL:                  20,
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

func TestIsExistTable(t *testing.T) {
	conn := getConnection(option)
	client := Client{
		conn: conn,
	}

	var (
		tableName = option.TableName
	)

	_, err := client.isExistTable(tableName)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateTable(t *testing.T) {
	conn := getConnection(option)
	client := Client{
		conn:      conn,
		tableName: option.TableName,
	}

	deleteTable(client)
	err := client.createTable(option.ReadCapacityUnits, option.WriteCapacityUnits)
	if err != nil {
		t.Fatal(err)
	}

	deleteTable(client)
}

func TestDescribeTimeToLive(t *testing.T) {
	conn := getConnection(option)
	client := Client{
		conn:      conn,
		tableName: option.TableName,
	}

	_ = client.createTable(option.ReadCapacityUnits, option.WriteCapacityUnits)
	_, err := client.describeTTL()
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdateTimeToLive(t *testing.T) {
	conn := getConnection(option)
	client := Client{
		conn:      conn,
		tableName: option.TableName,
	}

	_ = client.createTable(option.ReadCapacityUnits, option.WriteCapacityUnits)
	err := client.updateTTLSetting(ttlAttrName, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.updateTTLSetting(ttlAttrName, false)
	if err != nil {
		t.Fatal(err)
	}
	err = client.updateTTLSetting(ttlAttrName, true)
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
	found, err := client.Get(key, &value)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal(errors.New("not found"))
	}
}

func TestUpdateTTL(t *testing.T) {
	client, err := NewClient(option)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const (
		key = "test"
	)

	err = client.UpdateTTL(key)
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

// TestHelper
func deleteTable(client Client) {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(client.tableName),
	}
	_, _ = client.conn.DeleteTable(input)
}
