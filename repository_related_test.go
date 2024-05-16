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

func TestBuilder_Related(t *testing.T) {
	client, _ := newStubbedClient()

	_, err := dynamorm.NewBuilder[*examples.UserModel]().
		WithClient(client).
		WithTableName("users").
		WithModeler(examples.NewUserModeler()).
		Build()

	assert.Nil(t, err)
}

func TestGet_Related(t *testing.T) {
	client, stubber := newStubbedClient()

	stubber.Add(
		testtools.Stub{
			OperationName: "GetItem",
			Input: &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "001"},
				},
				TableName: aws.String("users"),
			},
			Output: &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"PK":       &types.AttributeValueMemberS{Value: "001"},
					"Username": &types.AttributeValueMemberS{Value: "jappleseed"},
					"Email":    &types.AttributeValueMemberS{Value: "john@appleseed.io"},
					"Type":     &types.AttributeValueMemberS{Value: "User"},
				},
			},
		},
	)

	repo, err := dynamorm.NewBuilder[*examples.UserModel]().
		WithClient(client).
		WithTableName("users").
		WithModeler(examples.NewUserModeler()).
		Build()

	key := dynamorm.Key{
		"PK": dynamorm.KeyValue("001"),
	}
	model, err := repo.Get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, key, model.Key())
}

func TestCreate_Related(t *testing.T) {
	client, stubber := newStubbedClient()
	repo, err := dynamorm.NewBuilder[*examples.UserModel]().
		WithClient(client).
		WithTableName("users").
		WithModeler(examples.NewUserModeler()).
		Build()
	assert.Nil(t, err)

	stubber.Add(
		testtools.Stub{
			OperationName: "TransactWriteItems",
			Input: &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							Item: map[string]types.AttributeValue{
								"PK":       &types.AttributeValueMemberS{Value: "002"},
								"Username": &types.AttributeValueMemberS{Value: "fherbert"},
								"Type":     &types.AttributeValueMemberS{Value: "User"},
							},
							TableName:           aws.String("users"),
							ConditionExpression: aws.String("attribute_not_exists (#0)"),
							ExpressionAttributeNames: map[string]string{
								"#0": "PK",
							},
						},
					},
					{
						Put: &types.Put{
							Item: map[string]types.AttributeValue{
								"PK":     &types.AttributeValueMemberS{Value: "fherbert"},
								"UserId": &types.AttributeValueMemberS{Value: "002"},
								"Type":   &types.AttributeValueMemberS{Value: "Username"},
							},
							TableName:           aws.String("users"),
							ConditionExpression: aws.String("attribute_not_exists (#0)"),
							ExpressionAttributeNames: map[string]string{
								"#0": "PK",
							},
						},
					},
				},
			},
			Output: &dynamodb.TransactWriteItemsOutput{},
		},
	)

	newUser := examples.NewUserWithIDAndUsername("002", "fherbert")
	err = repo.Create(context.Background(), newUser)
	assert.NoError(t, err)
}

func TestUpdate_Related(t *testing.T) {
	client, stubber := newStubbedClient()

	// Serves repo.Get()
	// Returns a user that has no username yet.
	stubber.Add(
		testtools.Stub{
			OperationName: "GetItem",
			Input: &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "001"},
				},
				TableName: aws.String("users"),
			},
			Output: &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "001"},
					// No username yet.
					// "Username": &types.AttributeValueMemberS{Value: "jappleseed"},
					"Type": &types.AttributeValueMemberS{Value: "User"},
				},
			},
		},
	)

	// Serves repo.Update()
	// Updates the user with a username.
	stubber.Add(
		testtools.Stub{
			OperationName: "TransactWriteItems",
			Input: &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							Item: map[string]types.AttributeValue{
								"PK":       &types.AttributeValueMemberS{Value: "001"},
								"Username": &types.AttributeValueMemberS{Value: "jappleseed"},
								"Type":     &types.AttributeValueMemberS{Value: "User"},
							},
							ConditionExpression: aws.String("attribute_exists (#0)"),
							ExpressionAttributeNames: map[string]string{
								"#0": "PK",
							},
							TableName: aws.String("users"),
						},
					},
					{
						Put: &types.Put{
							Item: map[string]types.AttributeValue{
								"PK":     &types.AttributeValueMemberS{Value: "jappleseed"},
								"UserId": &types.AttributeValueMemberS{Value: "001"},
								"Type":   &types.AttributeValueMemberS{Value: "Username"},
							},
							ConditionExpression: aws.String("attribute_not_exists (#0)"),
							ExpressionAttributeNames: map[string]string{
								"#0": "PK",
							},
							TableName: aws.String("users"),
						},
					},
				},
			},
			Output: &dynamodb.TransactWriteItemsOutput{},
		},
	)

	repo, err := dynamorm.NewBuilder[*examples.UserModel]().
		WithClient(client).
		WithTableName("users").
		WithModeler(examples.NewUserModeler()).
		Build()

	key := dynamorm.Key{
		"PK": dynamorm.KeyValue("001"),
	}
	model, err := repo.Get(context.Background(), key)
	assert.Nil(t, err)
	assert.Equal(t, key, model.Key())

	model.SetUsername("jappleseed")

	err = repo.Update(context.Background(), model)
	assert.NoError(t, err)
}
