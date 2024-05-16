package dynamorm

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Builder[T Model] struct {
	client    *dynamodb.Client
	tableName string
	modeler   func(item map[string]types.AttributeValue) (T, error)
}

func NewBuilder[T Model]() *Builder[T] {
	return &Builder[T]{}
}

func (b *Builder[T]) WithClient(client *dynamodb.Client) *Builder[T] {
	b.client = client
	return b
}

func (b *Builder[T]) WithTableName(tableName string) *Builder[T] {
	b.tableName = tableName
	return b
}

func (b *Builder[T]) WithModeler(modeler func(item map[string]types.AttributeValue) (T, error)) *Builder[T] {
	b.modeler = modeler
	return b
}

func (b *Builder[T]) Build() (Repository[T], error) {
	tableName := &b.tableName
	return &repositoryImpl[T]{
		client:    b.client,
		tableName: tableName,
		modeler:   b.modeler,
	}, nil
}
