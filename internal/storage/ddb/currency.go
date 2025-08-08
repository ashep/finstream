package ddb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ashep/finstream/internal/apperr"
	"github.com/ashep/go-banking"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/shopspring/decimal"
)

type currencyRateKey struct {
	PK string
	SK string
}

type currencyRate struct {
	PK         string    `dynamodbav:"PK"`
	SK         string    `dynamodbav:"SK"`
	Provider   string    `dynamodbav:"P"`
	BaseCode   string    `dynamodbav:"B"`
	TargetCode string    `dynamodbav:"T"`
	Date       time.Time `dynamodbav:"D"`
	Rate       float64   `dynamodbav:"R"`
}

func (d *DDB) GetCurrencyRate(ctx context.Context, provider, baseCode, targetCode string) (*banking.CurrencyRate, error) {
	k := getCurrencyRateKey(provider, baseCode, targetCode)
	getItemRes, err := d.cli.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: d.tableName,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: k.PK},
			"SK": &types.AttributeValueMemberS{Value: k.SK},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("ddb: %w", err)
	}

	if getItemRes.Item == nil {
		return nil, apperr.ErrCurrencyRateNotFound
	}

	return unmarshalCurrencyRate(getItemRes.Item)
}

func (d *DDB) SetCurrencyRate(ctx context.Context, rate banking.CurrencyRate) (bool, error) {
	exRate, err := d.GetCurrencyRate(ctx, rate.Provider, rate.Base.Code, rate.Target.Code)
	if err != nil && !errors.Is(err, apperr.ErrCurrencyRateNotFound) {
		return false, fmt.Errorf("get existing rate: %w", err)
	}

	if exRate != nil && rate.Date.Equal(exRate.Date) {
		return false, nil // no changes
	}

	item, err := marshalCurrencyRate(rate)
	if err != nil {
		return false, err
	}

	_, err = d.cli.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      item,
		TableName: d.tableName,
	})
	if err != nil {
		return false, fmt.Errorf("ddb: put item: %w", err)
	}

	return true, nil
}

func marshalCurrencyRate(rate banking.CurrencyRate) (map[string]types.AttributeValue, error) {
	k := getCurrencyRateKey(rate.Provider, rate.Base.Code, rate.Target.Code)

	item, err := attributevalue.MarshalMap(currencyRate{
		PK:         k.PK,
		SK:         k.SK,
		Provider:   rate.Provider,
		BaseCode:   rate.Base.Code,
		TargetCode: rate.Target.Code,
		Date:       rate.Date,
		Rate:       rate.Rate.InexactFloat64(),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	return item, nil
}

func unmarshalCurrencyRate(cr map[string]types.AttributeValue) (*banking.CurrencyRate, error) {
	item := &currencyRate{}
	if err := attributevalue.UnmarshalMap(cr, &item); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	base, err := banking.NewCurrencyByCode(item.BaseCode)
	if err != nil {
		return nil, fmt.Errorf("new base currency by num %d: %w", item.BaseCode, err)
	}
	target, err := banking.NewCurrencyByCode(item.TargetCode)
	if err != nil {
		return nil, fmt.Errorf("new target currency by num %d: %w", item.TargetCode, err)
	}

	return &banking.CurrencyRate{
		Provider: item.Provider,
		Base:     base,
		Target:   target,
		Date:     item.Date,
		Rate:     decimal.NewFromFloat(item.Rate),
	}, nil
}

func getCurrencyRateKey(provider, baseCode, targetCode string) currencyRateKey {
	return currencyRateKey{
		PK: fmt.Sprintf("CR"),
		SK: fmt.Sprintf("P/%s/B/%s/T/%s", provider, baseCode, targetCode),
	}
}
