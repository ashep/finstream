package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DDB struct {
	cli       *dynamodb.Client
	tableName *string
}

func New(region, tableName, accessKeyID, accessKeySecret string) *DDB {
	cfg := aws.Config{
		Region: region,
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: accessKeySecret,
			}, nil
		}),
	}

	return &DDB{
		cli:       dynamodb.NewFromConfig(cfg),
		tableName: aws.String(tableName),
	}
}
