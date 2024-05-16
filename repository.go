package dynamorm

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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
// The repository uses this function to convert the output of DynamoDB operations into a model.
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

	putItem, err := r.constructPutItem(model)
	if err != nil {
		return err
	}

	// In order to satisfy the "Create" operation, we need to ensure that the item does not already exist.
	// We do this by constructing a condition expression that asserts that the item does not exist.
	// We'll infer the proper expression from the model.Key()
	expr, err := key.CondtionExpressionForCreate()
	if err != nil {
		return err
	}
	putItem.ConditionExpression = expr.Condition()
	putItem.ExpressionAttributeNames = expr.Names()
	putItem.ExpressionAttributeValues = expr.Values()

	puts := []dynamodb.PutItemInput{*putItem}

	puts = r.appendPutsFromRelatedModels(puts, model)

	// If there's only one put, we can use PutItem.
	if len(puts) == 1 {
		_, err = r.client.PutItem(ctx, putItem)
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

	putItem, err := r.constructPutItem(model)
	if err != nil {
		return err
	}

	// In order to satisfy the "Update" operation, we need to ensure that the item already exists.
	// We'll infer the proper expression from the model.Key()
	expr, err := key.ConditionExpressionForUpdate()
	if err != nil {
		return err
	}

	putItem.ConditionExpression = expr.Condition()
	putItem.ExpressionAttributeNames = expr.Names()
	putItem.ExpressionAttributeValues = expr.Values()

	puts := []dynamodb.PutItemInput{*putItem}
	puts = r.appendPutsFromRelatedModels(puts, model)

	// If there's only one put, we can use PutItem.
	if len(puts) == 1 {
		_, err = r.client.PutItem(ctx, putItem)
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

	key := model.Key()
	// Merge the key into the item. This is necessary in case the key is not part of the item, or if
	// somehow they disagree. The key should always take precedence.
	for k, v := range key {
		item[k] = v
	}

	input.Item = item
	if expr := model.ConditionExpression(); expr != nil {
		input.ConditionExpression = expr.Condition()
		input.ExpressionAttributeNames = expr.Names()
		input.ExpressionAttributeValues = expr.Values()
	}
	return input, nil
}

func (r *repositoryImpl[T]) appendPutsFromRelatedModels(puts []dynamodb.PutItemInput, model Model) []dynamodb.PutItemInput {
	// Check if the model type supports related models.
	related, ok := Model(model).(HasRelated)
	if ok {
		relatedModels, err := related.Related()
		if err != nil {
			return puts
		}
		// TODO: Bredth-first search for related models, if we want to go beyond 1 layer deep.
		for _, rel := range relatedModels {
			relPut, err := r.constructPutItem(rel)
			if err != nil {
				return puts
			}
			puts = append(puts, *relPut)
		}
	}
	return puts
}
