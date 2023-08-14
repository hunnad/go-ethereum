package node

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	rdc *redis.Client
}

type PendingTransaction struct {
	BlockHash        *common.Hash      `json:"blockHash"`
	BlockNumber      *hexutil.Big      `json:"blockNumber"`
	From             common.Address    `json:"from"`
	Gas              hexutil.Uint64    `json:"gas"`
	GasPrice         *hexutil.Big      `json:"gasPrice"`
	GasFeeCap        *hexutil.Big      `json:"maxFeePerGas,omitempty"`
	GasTipCap        *hexutil.Big      `json:"maxPriorityFeePerGas,omitempty"`
	Hash             common.Hash       `json:"hash"`
	Input            hexutil.Bytes     `json:"input"`
	Nonce            hexutil.Uint64    `json:"nonce"`
	To               *common.Address   `json:"to"`
	TransactionIndex *hexutil.Uint64   `json:"transactionIndex"`
	Value            *hexutil.Big      `json:"value"`
	Type             hexutil.Uint64    `json:"type"`
	Accesses         *types.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big      `json:"chainId,omitempty"`
	V                *hexutil.Big      `json:"v"`
	R                *hexutil.Big      `json:"r"`
	S                *hexutil.Big      `json:"s"`
	YParity          *hexutil.Uint64   `json:"yParity,omitempty"`
}

type PendingTransactionEvent struct {
	ChainId     int                 `json:"chainId"`
	Timestamp   int64               `json:"timestamp"`
	From        string              `json:"from"`
	Transaction *PendingTransaction `json:"transaction"`
}

func newRedisClient(host string, port int, password string) *RedisClient {
	address := host + ":" + strconv.Itoa(port)
	rdc := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})

	return &RedisClient{
		rdc: rdc,
	}
}

func (client *RedisClient) SendPendingTransactions(txs []*types.Transaction) error {
	ctx := context.Background()
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for _, tx := range txs {
		pendingTx := client.newPendingTransaction(tx)
		event := &PendingTransactionEvent{
			ChainId:     137,
			Timestamp:   now,
			From:        "geth",
			Transaction: pendingTx,
		}
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		client.rdc.Publish(ctx, "chain.monitor.pending_transaction", string(data))
	}
	return nil
}

func (client *RedisClient) newPendingTransaction(tx *types.Transaction) *PendingTransaction {
	signer := types.NewLondonSigner(nil)
	from, _ := types.Sender(signer, tx)
	v, r, s := tx.RawSignatureValues()
	result := &PendingTransaction{
		Type:     hexutil.Uint64(tx.Type()),
		From:     from,
		Gas:      hexutil.Uint64(tx.Gas()),
		GasPrice: (*hexutil.Big)(tx.GasPrice()),
		Hash:     tx.Hash(),
		Input:    hexutil.Bytes(tx.Data()),
		Nonce:    hexutil.Uint64(tx.Nonce()),
		To:       tx.To(),
		Value:    (*hexutil.Big)(tx.Value()),
		V:        (*hexutil.Big)(v),
		R:        (*hexutil.Big)(r),
		S:        (*hexutil.Big)(s),
	}
	switch tx.Type() {
	case types.LegacyTxType:
		// if a legacy transaction has an EIP-155 chain id, include it explicitly
		if id := tx.ChainId(); id.Sign() != 0 {
			result.ChainID = (*hexutil.Big)(id)
		}

	case types.AccessListTxType:
		al := tx.AccessList()
		yparity := hexutil.Uint64(v.Sign())
		result.Accesses = &al
		result.ChainID = (*hexutil.Big)(tx.ChainId())
		result.YParity = &yparity

	case types.DynamicFeeTxType:
		al := tx.AccessList()
		yparity := hexutil.Uint64(v.Sign())
		result.Accesses = &al
		result.ChainID = (*hexutil.Big)(tx.ChainId())
		result.YParity = &yparity
		result.GasFeeCap = (*hexutil.Big)(tx.GasFeeCap())
		result.GasTipCap = (*hexutil.Big)(tx.GasTipCap())
		result.GasPrice = (*hexutil.Big)(tx.GasFeeCap())
	}
	return result
}
