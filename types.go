package dynamorm

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Model interface {
	// Returns the struct that gets marshalled into the DynamoDB item.
	Item() interface{}

	// Will be merged with the item returned by Item() before saving.
	Key() Key

	// Returns any condition expression that should be applied to save operations.
	ConditionExpression() *expression.Expression
}

type HasRelated interface {
	Related() ([]Model, error)
}

type Repository[T Model] interface {
	// Retrieves a single item from DynamoDB by key.
	Get(ctx context.Context, key Key) (T, error)

	// Writes a single item to DynamoDB, transactionally with its relations.
	Create(ctx context.Context, model T) error

	// Writes a single item to DynamoDB, transactionally with its relations.
	Update(ctx context.Context, model T) error
}

type Key map[string]types.AttributeValue

func KeyValue(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
}

type HasConditionExpression struct {
	conditionExpression *expression.Expression
}

func (s *HasConditionExpression) SetConditionExpression(expr *expression.Expression) {
	s.conditionExpression = expr
}

func (s *HasConditionExpression) ConditionExpression() *expression.Expression {
	return s.conditionExpression
}
