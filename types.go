package dynamorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Model is an interface that models must implement to be saved to DynamoDB.
type Model interface {
	// Item Returns the struct that gets marshalled into the DynamoDB item.
	// It can contain the primary keys along with other attributes, but note that Key() will be merged with this for
	// sanity.
	Item() interface{}

	// Key returns the primary key of the model.
	// This will be merged with the result of Item() to form the final item that gets saved to DynamoDB.
	// The key is used to uniquely identify the item in the table, and will be used to construct
	// condition expressions for save operations.
	Key() Key

	// ConditionExpression Any condition expression that should be applied to save operations.
	ConditionExpression() *expression.Expression
}

// HasRelated is an optional interface that models can implement if it provide related models that should be saved
type HasRelated interface {
	Model
	Related() ([]Model, error)
}

type Repository[T Model] interface {
	// Retrieves a single item from DynamoDB by key.
	Get(ctx context.Context, key Key) (T, error)

	// Create Creates a single item to DynamoDB, transactionally with its relations.
	// Uses the Put operation to save the item, with a condition expression that asserts that the item does not yet exist.
	Create(ctx context.Context, model T) error

	// Update Updates a single item to DynamoDB, transactionally with its relations.
	// Uses the Put operation to save the item, with a condition expression that asserts that the item already exists.
	Update(ctx context.Context, model T) error
}

// Key is a map of attribute names to attribute values.
// It is used to represent the primary key of a DynamoDB item.
// We use this type alias to provide extra methods, like generating condition expressions
// based on what the model's key is.
type Key map[string]types.AttributeValue

// KeyValue is a helper function that constructs a string attribute value.
func KeyValue(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
}

// HasConditionExpression is a convenience struct that models can embed to
// provide a setter-getter for condition expressions.
type HasConditionExpression struct {
	conditionExpression *expression.Expression
}

func (s *HasConditionExpression) SetConditionExpression(expr *expression.Expression) {
	s.conditionExpression = expr
}

func (s *HasConditionExpression) ConditionExpression() *expression.Expression {
	return s.conditionExpression
}

// Given a model's Key(), construct a condition expression that asserts that the item does not exist.
func (key Key) CondtionExpressionForCreate() (*expression.Expression, error) {
	conditions := []expression.ConditionBuilder{}
	for k := range key {
		conditions = append(conditions, expression.AttributeNotExists(expression.Name(k)))
	}

	var exprBuilder expression.Builder
	if len(conditions) == 1 {
		exprBuilder = expression.NewBuilder().WithCondition(conditions[0])
	} else {
		exprBuilder = expression.NewBuilder().WithCondition(
			expression.And(conditions[0], conditions[1]),
		)
	}
	expr, err := exprBuilder.Build()
	if err != nil {
		return nil, err
	}
	return &expr, err
}

// Given a model's Key(), construct a condition expression that asserts that the item does not exist.
func (key Key) ConditionExpressionForUpdate() (*expression.Expression, error) {
	conditions := []expression.ConditionBuilder{}
	for k := range key {
		conditions = append(conditions, expression.AttributeExists(expression.Name(k)))
	}
	var exprBuilder expression.Builder
	if len(conditions) == 1 {
		exprBuilder = expression.NewBuilder().WithCondition(conditions[0])
	} else {
		exprBuilder = expression.NewBuilder().WithCondition(
			expression.And(conditions[0], conditions[1]),
		)
	}
	expr, err := exprBuilder.Build()
	if err != nil {
		return nil, err
	}
	return &expr, err
}
