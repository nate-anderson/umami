package umami

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/exp/constraints"
)

// Document defines a storable document
type Document[PartitionT, SortT constraints.Ordered] interface {
	Key() (PartitionT, *SortT)
}

// Repository manages persistence for one type
type Repository[DocT Document[PartitionT, SortT], PartitionT, SortT constraints.Ordered] struct {
	client       *dynamodb.Client
	table        string
	partitionKey string
	sortKey      *string
}

// New instantiates a repository
func New[DocT Document[PartitionT, SortT], PartitionT, SortT constraints.Ordered](client *dynamodb.Client, table, partitionKey string, sortKey *string) *Repository[DocT, PartitionT, SortT] {
	return &Repository[DocT, PartitionT, SortT]{
		client,
		table,
		partitionKey,
		sortKey,
	}
}

// Get item
func (r Repository[DocT, PartitionT, SortT]) Get(ctx context.Context, partition PartitionT, sort *SortT) (DocT, error) {
	var dest DocT
	var err error

	key := make(map[string]types.AttributeValue)
	key[r.partitionKey], err = attributevalue.Marshal(partition)
	if err != nil {
		return dest, err
	}

	if r.sortKey != nil {
		key[*r.sortKey], err = attributevalue.Marshal(sort)
	}

	input := &dynamodb.GetItemInput{
		TableName: &r.table,
		Key:       key,
	}

	result, err := r.client.GetItem(ctx, input)
	if err != nil {
		return dest, err
	}

	err = attributevalue.UnmarshalMap(result.Item, &dest)
	return dest, err
}

func (r Repository[DocT, ParitionT, SortT]) Store(ctx context.Context, document DocT) error {
	attrs, err := attributevalue.MarshalMap(document)
	if err != nil {
		return err
	}

	pk, sk := document.Key()

	// explicitly set partition key and, if necessary, sort key values
	attrs[r.partitionKey], err = attributevalue.Marshal(pk)
	if err != nil {
		return err
	}
	if sk != nil {
		attrs[*r.sortKey], err = attributevalue.Marshal(*sk)
		if err != nil {
			return err
		}
	}

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &r.table,
		Item:      attrs,
	})

	return err
}

func Pointer[T any](value T) *T {
	return &value
}
