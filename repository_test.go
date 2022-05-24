package umami

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type testConfig struct {
	Host         string  `json:"host"`
	Port         int     `json:"port"`
	Table        string  `json:"table"`
	PartitionKey string  `json:"partitionKey"`
	SortKey      *string `json:"sortKey"`
}

type testDocument struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func (td testDocument) Key() (string, *int) {
	return td.Email, Pointer(0)
}

func (td testDocument) AttributeNames() []string {
	return []string{"name", "email"}
}

const configPath = "./test_config.json"

func TestRepository(t *testing.T) {
	configFile, err := os.Open(configPath)
	if err != nil {
		panic(err)
	}

	testConf := testConfig{}
	configDecoder := json.NewDecoder(configFile)
	err = configDecoder.Decode(&testConf)
	if err != nil {
		panic(err)
	}

	ddbConfig, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-west-2"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://%s:%d", testConf.Host, testConf.Port)}, nil
			},
		)),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "dummy",
				SecretAccessKey: "dummy",
				SessionToken:    "dummy",
				Source:          "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)

	ctx := context.Background()
	cl := dynamodb.NewFromConfig(ddbConfig)

	// create table if not exists
	_, err = cl.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &testConf.Table,
	})

	if err != nil {
		var dynamoErr *types.ResourceInUseException
		if !errors.As(err, &dynamoErr) {
			panic(err)
		} else {
			log.Println("Table exists: continuing")
			_, err = cl.CreateTable(ctx, &dynamodb.CreateTableInput{
				TableName: &testConf.Table,
				AttributeDefinitions: []types.AttributeDefinition{
					{
						AttributeName: &testConf.PartitionKey,
						AttributeType: types.ScalarAttributeTypeS,
					},
					{
						AttributeName: testConf.SortKey,
						AttributeType: types.ScalarAttributeTypeN,
					},
				},
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: &testConf.PartitionKey,
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: testConf.SortKey,
						KeyType:       types.KeyTypeRange,
					},
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  Pointer[int64](5),
					WriteCapacityUnits: Pointer[int64](5),
				},
			})
		}
	}

	if err != nil {
		panic(err)
	}

	repo := New[testDocument, string, int](cl, testConf.Table, testConf.PartitionKey, testConf.SortKey)

	t.Run("store item in repository", func(t *testing.T) {
		ctx := context.Background()
		myDoc := testDocument{
			Name:  "Test User",
			Email: "admin@example.com",
		}

		err := repo.Store(ctx, myDoc)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("find stored item", func(t *testing.T) {
		ctx := context.Background()
		myDoc, err := repo.Get(ctx, "admin@example.com", Pointer(0))
		if err != nil {
			t.Fatal(err)
		}

		if myDoc.Email != "admin@example.com" {
			t.Fatalf("expected email admin@example.com, got %s", myDoc.Email)
		}
	})
}
