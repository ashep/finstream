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
	PK        string    `dynamodbav:"PK"`
	SK        string    `dynamodbav:"SK"`
	BaseNum   int       `dynamodbav:"B"`
	TargetNum int       `dynamodbav:"T"`
	Date      time.Time `dynamodbav:"D"`
	Rate      float64   `dynamodbav:"R"`
}

func (d *DDB) GetCurrencyRate(ctx context.Context, baseNum, targetNum int) (*banking.CurrencyRate, error) {
	k := getCurrencyRateKey(baseNum, targetNum)
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

	return toBankingCurrencyRate(getItemRes.Item)
}

func (d *DDB) SetCurrencyRate(ctx context.Context, rate banking.CurrencyRate) (bool, error) {
	exRate, err := d.GetCurrencyRate(ctx, rate.Base.Num, rate.Target.Num)
	if err != nil && !errors.Is(err, apperr.ErrCurrencyRateNotFound) {
		return false, fmt.Errorf("get existing rate: %w", err)
	}

	if exRate != nil && rate.Date.Equal(exRate.Date) && rate.Rate.Equal(exRate.Rate) {
		return false, nil // no changes
	}

	item, err := fromBankingCurrencyRate(rate)
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

func fromBankingCurrencyRate(rate banking.CurrencyRate) (map[string]types.AttributeValue, error) {
	k := getCurrencyRateKey(rate.Base.Num, rate.Target.Num)

	item, err := attributevalue.MarshalMap(currencyRate{
		PK:        k.PK,
		SK:        k.SK,
		BaseNum:   rate.Base.Num,
		TargetNum: rate.Target.Num,
		Date:      rate.Date,
		Rate:      rate.Rate.InexactFloat64(),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	return item, nil
}

func toBankingCurrencyRate(cr map[string]types.AttributeValue) (*banking.CurrencyRate, error) {
	item := &currencyRate{}
	if err := attributevalue.UnmarshalMap(cr, &item); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	base, err := banking.NewCurrencyByNum(item.BaseNum)
	if err != nil {
		return nil, fmt.Errorf("new base currency by num %d: %w", item.BaseNum, err)
	}
	target, err := banking.NewCurrencyByNum(item.TargetNum)
	if err != nil {
		return nil, fmt.Errorf("new target currency by num %d: %w", item.TargetNum, err)
	}

	return &banking.CurrencyRate{
		Base:   base,
		Target: target,
		Date:   item.Date,
		Rate:   decimal.NewFromFloat(item.Rate),
	}, nil
}

func getCurrencyRateKey(baseNum, targetNum int) currencyRateKey {
	return currencyRateKey{
		PK: fmt.Sprintf("BCR/%d", baseNum),
		SK: fmt.Sprintf("TCR/%d", targetNum),
	}
}
