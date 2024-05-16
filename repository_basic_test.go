package dynamorm_test

import (
	"context"
	"testing"

	"github.com/bezhermoso/dynamorm"
	"github.com/bezhermoso/dynamorm/internal/examples"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/testtools"
	"github.com/stretchr/testify/assert"
)

func newStubbedClient() (*dynamodb.Client, *testtools.AwsmStubber) {
	stubber := testtools.NewStubber()
	return dynamodb.NewFromConfig(*stubber.SdkConfig, func(o *dynamodb.Options) {
		o.IdempotencyTokenProvider = nil
	}), stubber
}

func TestBuilder(t *testing.T) {

	client, _ := newStubbedClient()

	_, err := dynamorm.NewBuilder[*examples.BasicModel]().
		WithClient(client).
		WithTableName("people").
		WithModeler(examples.NewBasicModeler()).
		Build()

	assert.Nil(t, err)
}

func TestGet(t *testing.T) {
	client, stubber := newStubbedClient()
	stubber.Add(
		testtools.Stub{
			OperationName: "GetItem",
			Input: &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "ABC"},
					"SK": &types.AttributeValueMemberS{Value: "123"},
				},
				TableName: aws.String("people"),
			},
			Output: &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"PK":      &types.AttributeValueMemberS{Value: "ABC"},
					"SK":      &types.AttributeValueMemberS{Value: "123"},
					"Name":    &types.AttributeValueMemberS{Value: "John Appleseed"},
					"Age":     &types.AttributeValueMemberN{Value: "30"},
					"Hobbies": &types.AttributeValueMemberSS{Value: []string{"reading", "coding"}},
				},
			},
		},
	)

	repo, err := dynamorm.NewBuilder[*examples.BasicModel]().
		WithClient(client).
		WithTableName("people").
		WithModeler(examples.NewBasicModeler()).
		Build()

	key := dynamorm.Key{
		"PK": dynamorm.KeyValue("ABC"),
		"SK": dynamorm.KeyValue("123"),
	}
	model, err := repo.Get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, key, model.Key())
}
