package examples

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bezhermoso/dynamorm"
)

type dto struct {
	PK      string   `dynamodbav:"PK"`
	SK      string   `dynamodbav:"SK"`
	Name    string   `dynamodbav:"Name"`
	Age     int      `dynamodbav:"Age"`
	Hobbies []string `dynamodbav:"Hobbies"`
}

type BasicModel struct {
	dto                 *dto
	conditionExpression *expression.Expression
}

// ConditionExpression implements dynamorm.Model.
func (b *BasicModel) ConditionExpression() *expression.Expression {
	return b.conditionExpression
}

// Item implements dynamorm.Model.
func (b *BasicModel) Item() interface{} {
	return b.dto
}

// Key implements dynamorm.Model.
func (b *BasicModel) Key() dynamorm.Key {
	return dynamorm.Key{
		"PK": dynamorm.KeyValue(b.dto.PK),
		"SK": dynamorm.KeyValue(b.dto.SK),
	}
}

func (b *BasicModel) SetConditionExpression(expr *expression.Expression) {
	b.conditionExpression = expr
}

func NewBasicModeler() dynamorm.Modeler[*BasicModel] {
	return func(item map[string]types.AttributeValue) (*BasicModel, error) {
		dto := &dto{}
		err := attributevalue.UnmarshalMap(item, dto)
		if err != nil {
			return nil, err
		}
		return &BasicModel{dto: dto}, nil
	}
}

var _ dynamorm.Model = &BasicModel{}
