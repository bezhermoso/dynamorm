package examples

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bezhermoso/dynamorm"
)

type userDto struct {
	ID       string `dynamodbav:"PK"`
	Name     string `dynamodbav:"Name"`
	Username string `dynamodbav:"Username"`
	Type     string `dynamodbav:"Type"`
}

type usernameDto struct {
	Username string `dynamodbav:"PK"`
	Type     string `dynamodbav:"Type"`
	UserId   string `dynamodbav:"UserId"`
}

func NewWithDetails(id, name, username string) *UserModel {
	u := &UserModel{
		dto: &userDto{
			ID:       id,
			Name:     name,
			Username: username,
			Type:     "User",
		},
		// Unless the user was fetched from DynamoDB, we should assume that there isn't a prior username
		// associated with the user yet.
		priorUsername: "",
	}
	return u
}

type UserModel struct {
	// The user item that will be saved to DynamoDB.
	dto *userDto
	// The username that is associated with the user.
	priorUsername string
}

// Related implements dynamorm.HasRelated.
// This is called whenever a user item is being saved. It provides the related username item that
// should be saved along with the user item within a single transaction.
// The username item associates the user with a unique username.
func (u *UserModel) Related() ([]dynamorm.Model, error) {

	related := make([]dynamorm.Model, 0, 1)

	if u.dto.Username != "" {
		// The user has a username associated with it. We need to figure out if its new or existing.
		username := usernameModelFromUserDto(u.dto)
		if u.priorUsername == "" {
			// There was no username associated with the user before.
			// Seed the condition expression that attests that the username item does not exist.
			expr, err := conditionExpressionForNewUsername(username)
			if err != nil {
				return nil, err
			}
			username.SetConditionExpression(expr)
			related = append(related, username)
		} else if u.dto.Username != u.priorUsername {
			// We don't allow the username to be changed.
			return nil, errors.New("username cannot be changed")
		} else {
			// The username associated with the user has not changed.
			// We already seeded the condition expression that attests that the username item exists.
			expr, err := conditionExpressionForExistingUsername(username)
			if err != nil {
				return nil, err
			}
			username.SetConditionExpression(expr)
			related = append(related, username)
		}
	}
	return related, nil
}

// ConditionExpression implements dynamorm.Model.
func (u *UserModel) ConditionExpression() *expression.Expression {
	return nil
}

// Item implements dynamorm.Model.
func (u *UserModel) Item() interface{} {
	return u.dto
}

// Key implements dynamorm.Model.
func (u *UserModel) Key() dynamorm.Key {
	return dynamorm.Key{
		"PK": dynamorm.KeyValue(u.dto.ID),
	}
}

type usernameModel struct {
	// The username item that will be saved to DynamoDB.
	dto *usernameDto
	// Embed the HasConditionExpression to provide a setter for conditional expressions.
	*dynamorm.HasConditionExpression
}

// Item implements dynamorm.Model.
func (u *usernameModel) Item() interface{} {
	return u.dto
}

// Key implements dynamorm.Model.
func (u *usernameModel) Key() dynamorm.Key {
	return dynamorm.Key{
		"PK": dynamorm.KeyValue(u.dto.Username),
	}
}

func NewUserModeler() dynamorm.Modeler[*UserModel] {
	// Called by dynamorm.Repository to create a new model instance from a DynamoDB item result.
	return func(item map[string]types.AttributeValue) (*UserModel, error) {
		dto := &userDto{}
		err := attributevalue.UnmarshalMap(item, dto)
		if err != nil {
			return nil, err
		}
		if dto.Type != "User" {
			return nil, dynamorm.IncompatibleModelerError
		}

		return &UserModel{
			dto:           dto,
			priorUsername: dto.Username,
		}, nil
	}
}

func (u *UserModel) SetUsername(username string) {
	u.dto.Username = username
}

func (u *UserModel) SetName(name string) {
	u.dto.Name = name
}

// Persisted As the user of the Repository, you should call this method after a successful save operation.
//
// The Repository cannot be responsible for this because it may not have the ability to determine
// which related models were saved or not, in cases where partial failures occur.

// Here we reset the priorUsernameModel to the username model that was assume was saved.
// Since we normally only save a model once, this shouldn't be needed in most cases.
func (u *UserModel) Persisted() error {
	u.priorUsername = u.dto.Username
	return nil
}

func usernameModelFromUserDto(dto *userDto) (username *usernameModel) {
	if dto.Username != "" {
		username = &usernameModel{
			HasConditionExpression: &dynamorm.HasConditionExpression{},
			dto: &usernameDto{
				Username: dto.Username,
				Type:     "Username",
				UserId:   dto.ID,
			},
		}
	}
	return username
}

func conditionExpressionForExistingUsername(u *usernameModel) (*expression.Expression, error) {
	// Attests that:
	//  1.) The username item exists.
	//  2.) The username item is associated with the user.
	expr, err := expression.NewBuilder().WithCondition(
		expression.And(
			expression.AttributeExists(expression.Name("PK")),
			expression.Equal(expression.Name("UserId"), expression.Value(u.dto.UserId)),
		),
	).Build()
	if err != nil {
		return nil, err
	}
	return &expr, nil
}

func conditionExpressionForNewUsername(*usernameModel) (*expression.Expression, error) {
	// Attests that:
	//  1.) The username is not taken: username item w/ PK = username does not exist.
	expr, err := expression.NewBuilder().WithCondition(
		expression.AttributeNotExists(expression.Name("PK")),
	).Build()
	if err != nil {
		return nil, err
	}
	return &expr, nil
}

var _ dynamorm.HasRelated = &UserModel{}
var _ dynamorm.Model = &usernameModel{}
