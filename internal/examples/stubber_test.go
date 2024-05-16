package examples

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/awsdocs/aws-doc-sdk-examples/gov2/testtools"
)

func newStubbedClient() (*dynamodb.Client, *testtools.AwsmStubber) {
	stubber := testtools.NewStubber()
	return dynamodb.NewFromConfig(*stubber.SdkConfig, func(o *dynamodb.Options) {
		o.IdempotencyTokenProvider = nil
	}), stubber
}
