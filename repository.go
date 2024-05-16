package dynamorm

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type repositoryImpl[T Model] struct {
	// The DynamoDB client.
	client *dynamodb.Client
	// The name of the DynamoDB table.
	tableName *string
	// The function that converts a map of attribute values into a Model instance.
	modeler Modeler[T]
}

// Modeler is a function that converts a map of attribute values into a model.
type Modeler[T Model] func(item map[string]types.AttributeValue) (T, error)

// TransactSaveMany implements Repository.
func (r *repositoryImpl[T]) Get(ctx context.Context, key Key) (T, error) {
	// Zero value of T.
	var result T
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		Key:       key,
		TableName: r.tableName,
	})
	if err != nil {
		return result, err
	}

	if len(out.Item) == 0 {
		return result, ErrNotFound
	}

	// Convert the item to a model.
	result, err = r.modeler(out.Item)
	if err != nil {
		return result, err
	}

	return result, nil
}

// TransactSaveMany implements Repository.
func (r *repositoryImpl[T]) Create(ctx context.Context, model T) error {

	key := model.Key()
	if key == nil || len(key) == 0 {
		return errors.New("key is required")
	}

	mainPut, err := r.constructPutItem(model)
	if err != nil {
		return err
	}

	// In order to satisfy the "Create" operation, we need to ensure that the item does not already exist.
	// We do this by constructing a condition expression that asserts that the item does not exist.
	// We'll infer the proper expression from the model.Key()
	expr, err := key.condtionExpressionForNew()
	if err != nil {
		return err
	}

	mainPut.ConditionExpression = expr.Condition()
	mainPut.ExpressionAttributeNames = expr.Names()
	mainPut.ExpressionAttributeValues = expr.Values()

	puts := []dynamodb.PutItemInput{*mainPut}

	// If the model has related models, we'll save those as well.
	related, ok := Model(model).(HasRelated)
	if ok {
		// TODO: Bredth-first search for related models, if we want to go beyond 1 layer deep.
		relatedModels, err := related.Related()
		if err != nil {
			return err
		}
		for _, rel := range relatedModels {
			relPut, err := r.constructPutItem(rel)
			if err != nil {
				return err
			}
			puts = append(puts, *relPut)
		}
	}

	// If there's only one put, we can use PutItem.
	if len(puts) == 1 {
		_, err = r.client.PutItem(ctx, mainPut)
		if err != nil {
			return err
		}
	} else {
		// Otherwise, we'll use TransactWriteItems.
		input := &dynamodb.TransactWriteItemsInput{
			TransactItems: make([]types.TransactWriteItem, 0),
		}
		for _, put := range puts {
			item := types.TransactWriteItem{
				Put: &types.Put{
					Item:                      put.Item,
					TableName:                 put.TableName,
					ConditionExpression:       put.ConditionExpression,
					ExpressionAttributeNames:  put.ExpressionAttributeNames,
					ExpressionAttributeValues: put.ExpressionAttributeValues,
				},
			}
			input.TransactItems = append(input.TransactItems, item)
		}
		_, err := r.client.TransactWriteItems(ctx, input)
		if err != nil {
			return err
		}
	}

	return nil
}

// TransactSaveMany implements Repository.
func (r *repositoryImpl[T]) Update(ctx context.Context, model T) error {

	key := model.Key()
	if key == nil || len(key) == 0 {
		return errors.New("key is required")
	}

	mainPut, err := r.constructPutItem(model)
	if err != nil {
		return err
	}

	// In order to satisfy the "Update" operation, we need to ensure that the item already exists.
	// We'll infer the proper expression from the model.Key()
	expr, err := key.conditionExpressionForUpdate()
	if err != nil {
		return err
	}

	mainPut.ConditionExpression = expr.Condition()
	mainPut.ExpressionAttributeNames = expr.Names()
	mainPut.ExpressionAttributeValues = expr.Values()

	puts := []dynamodb.PutItemInput{*mainPut}

	// If the model has related models, we'll save those as well.
	related, ok := Model(model).(HasRelated)
	if ok {
		// TODO: Bredth-first search for related models, if we want to go beyond 1 layer deep.
		relatedModels, err := related.Related()
		if err != nil {
			return err
		}
		for _, rel := range relatedModels {
			relPut, err := r.constructPutItem(rel)
			if err != nil {
				return err
			}
			puts = append(puts, *relPut)
		}
	}

	// If there's only one put, we can use PutItem.
	if len(puts) == 1 {
		_, err = r.client.PutItem(ctx, mainPut)
		if err != nil {
			return err
		}
	} else {
		// Otherwise, we'll use TransactWriteItems.
		input := &dynamodb.TransactWriteItemsInput{
			TransactItems: make([]types.TransactWriteItem, 0),
		}
		for _, put := range puts {
			item := types.TransactWriteItem{
				Put: &types.Put{
					Item:                      put.Item,
					TableName:                 put.TableName,
					ConditionExpression:       put.ConditionExpression,
					ExpressionAttributeNames:  put.ExpressionAttributeNames,
					ExpressionAttributeValues: put.ExpressionAttributeValues,
				},
			}
			input.TransactItems = append(input.TransactItems, item)
		}
		_, err := r.client.TransactWriteItems(ctx, input)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *repositoryImpl[T]) constructPutItem(model Model) (*dynamodb.PutItemInput, error) {
	input := &dynamodb.PutItemInput{}
	input.TableName = r.tableName
	item, err := attributevalue.MarshalMap(model.Item())
	if err != nil {
		return nil, err
	}
	input.Item = item
	if expr := model.ConditionExpression(); expr != nil {
		input.ConditionExpression = expr.Condition()
		input.ExpressionAttributeNames = expr.Names()
		input.ExpressionAttributeValues = expr.Values()
	}
	return input, nil
}

func (key Key) condtionExpressionForNew() (*expression.Expression, error) {
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

func (key Key) conditionExpressionForUpdate() (*expression.Expression, error) {
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
