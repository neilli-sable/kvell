package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/neilli-sable/kvell"
)

var keyAttrName = "k"
var valAttrName = "v"
var ttlAttrName = "unixtime"

// Client ...
type Client struct {
	conn      *dynamodb.DynamoDB
	tableName string
	ttl       time.Duration
}

// Option ...
type Option struct {
	Region               string
	TableName            string
	ReadCapacityUnits    int64
	WriteCapacityUnits   int64
	WaitForTableCreation *bool
	AWSAccessKeyID       string
	AWSSecretAccessKey   string
	CustomEndpoint       string
	TTL                  time.Duration
}

// TTL ...
type TTL struct {
	ttlAttributeName string
	enabled          bool
}

// NewClient ...
func NewClient(opt Option) (kvell.Store, error) {
	if opt.TableName == "" {
		return nil, errors.New("tableName is empty")
	}

	conn := getConnection(opt)
	client := &Client{
		conn:      conn,
		tableName: opt.TableName,
		ttl:       opt.TTL,
	}

	err := client.init(opt)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getConnection(opt Option) *dynamodb.DynamoDB {
	var creds *credentials.Credentials
	if opt.AWSAccessKeyID != "" && opt.AWSSecretAccessKey != "" {
		creds = credentials.NewStaticCredentials(opt.AWSAccessKeyID, opt.AWSSecretAccessKey, "")
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(opt.Region),
		Endpoint:    aws.String(opt.CustomEndpoint),
		Credentials: creds,
	})
	if err != nil {
		panic(err)
	}
	return dynamodb.New(sess)
}

func (c *Client) init(opt Option) error {
	// table create if not exist
	found, err := c.isExistTable(c.tableName)
	if err != nil {
		return err
	}
	if !found {
		err = c.createTable(opt.ReadCapacityUnits, opt.WriteCapacityUnits)
		if err != nil {
			return err
		}
	}

	// ttl setting
	current, err := c.describeTTL()
	// disabled to enable
	if opt.TTL.Nanoseconds() > 0 && !current.enabled {
		err = c.updateTTLSetting(ttlAttrName, true)
		if err != nil {
			return err
		}
	}
	// want disable
	if opt.TTL.Nanoseconds() == 0 && current.enabled {
		err = c.updateTTLSetting(ttlAttrName, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// Health ...
func (c *Client) Health() error {
	exist, err := c.isExistTable(c.tableName)
	if err != nil {
		return err
	}
	if !exist {
		return errors.New("table not found")
	}
	return nil
}

// isExistTable ...
func (c *Client) isExistTable(tableName string) (bool, error) {
	if tableName == "" {
		return false, errors.New("tableName is empty")
	}

	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName), // テーブル名
	}

	_, err := c.conn.DescribeTable(input)
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if !ok {
			return false, err
		} else if awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return false, nil
		}
	}
	return true, nil
}

// createTable ...
func (c *Client) createTable(readCapacityUnits, writeCapacityUnits int64) error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(keyAttrName),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(keyAttrName),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacityUnits),
			WriteCapacityUnits: aws.Int64(writeCapacityUnits),
		},
		TableName: aws.String(c.tableName),
	}

	_, err := c.conn.CreateTable(input)
	return err
}

// describeTTL ...
func (c *Client) describeTTL() (current TTL, err error) {
	input := &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(c.tableName),
	}

	result, err := c.conn.DescribeTimeToLive(input)
	if err != nil {
		return TTL{}, err
	}

	if result.TimeToLiveDescription.AttributeName != nil {
		current.ttlAttributeName = *result.TimeToLiveDescription.AttributeName
	}
	if result.TimeToLiveDescription.TimeToLiveStatus != nil {
		if *result.TimeToLiveDescription.TimeToLiveStatus == "ENABLED" {
			current.enabled = true
		} else {
			current.enabled = false
		}
	}

	return current, nil
}

// updateTTl
func (c *Client) updateTTLSetting(ttlAttributeName string, enable bool) error {
	ttl := &dynamodb.TimeToLiveSpecification{
		AttributeName: aws.String(ttlAttributeName),
		Enabled:       aws.Bool(enable),
	}
	input := &dynamodb.UpdateTimeToLiveInput{
		TableName:               aws.String(c.tableName), // テーブル名
		TimeToLiveSpecification: ttl,
	}

	_, err := c.conn.UpdateTimeToLive(input)
	return err
}

// Set ...
func (c *Client) Set(key string, value interface{}) error {
	if key == "" {
		return errors.New("key is empty")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	item := make(map[string]*dynamodb.AttributeValue)
	item[keyAttrName] = &dynamodb.AttributeValue{
		S: aws.String(key),
	}
	item[valAttrName] = &dynamodb.AttributeValue{
		B: data,
	}
	ttlUnixtime := time.Now().Add(c.ttl).Unix()
	item[ttlAttrName] = &dynamodb.AttributeValue{
		N: aws.String(fmt.Sprintf("%d", ttlUnixtime)),
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	}
	_, err = c.conn.PutItem(input)
	return err
}

// Get ...
func (c *Client) Get(key string, value interface{}) (found bool, err error) {
	keyAttr := make(map[string]*dynamodb.AttributeValue)
	keyAttr[keyAttrName] = &dynamodb.AttributeValue{
		S: aws.String(key),
	}
	getItemInput := dynamodb.GetItemInput{
		TableName: &c.tableName,
		Key:       keyAttr,
	}
	getItemOutput, err := c.conn.GetItem(&getItemInput)
	if err != nil {
		return false, err
	} else if getItemOutput.Item == nil {
		// Return false if the key-value pair doesn't exist
		return false, nil
	}
	attributeVal := getItemOutput.Item[valAttrName]
	if attributeVal == nil {
		return false, nil
	}
	data := attributeVal.B

	return true, json.Unmarshal(data, value)
}

// UpdateTTL ...
func (c *Client) UpdateTTL(key string) error {
	if key == "" {
		return errors.New("key is empty")
	}

	primaryKey := make(map[string]*dynamodb.AttributeValue)
	primaryKey[keyAttrName] = &dynamodb.AttributeValue{
		S: aws.String(key),
	}

	ttlUnixtime := time.Now().Add(c.ttl).Unix()
	attributeVal := make(map[string]*dynamodb.AttributeValue)
	attributeVal[":t"] = &dynamodb.AttributeValue{
		N: aws.String(fmt.Sprintf("%d", ttlUnixtime)),
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(c.tableName),
		Key:                       primaryKey,
		UpdateExpression:          aws.String("SET unixtime = :t"),
		ExpressionAttributeValues: attributeVal,
	}
	_, err := c.conn.UpdateItem(input)
	return err
}

// Delete ...
func (c *Client) Delete(key string) error {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			keyAttrName: {
				S: aws.String(key),
			},
		}}
	_, err := c.conn.DeleteItem(input)
	return err
}

// Close ...
func (c *Client) Close() error {
	// unnecessary close
	return nil
}
