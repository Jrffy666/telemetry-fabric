package evm

import (
	"fmt"
	"strings"
	"testing"
)

const (
	testUSDC     = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	testERC721   = "0x7777777777777777777777777777777777777777"
	testERC1155  = "0x5555555555555555555555555555555555555555"
	testUnknown  = "0x9999999999999999999999999999999999999999"
	testAlice    = "0x1111111111111111111111111111111111111111"
	testBob      = "0x2222222222222222222222222222222222222222"
	testCarol    = "0x3333333333333333333333333333333333333333"
	testZeroAddr = "0x0000000000000000000000000000000000000000"
)

func testTopicAddress(address string) string {
	address = strings.TrimPrefix(normalizeAddress(address), "0x")
	return "0x" + strings.Repeat("0", 24) + address
}

func testWordUint(value uint64) string {
	return fmt.Sprintf("0x%064x", value)
}

func testWordHex(hexValue string) string {
	hexValue = strings.TrimPrefix(normalizeHex(hexValue), "0x")
	if len(hexValue) >= 64 {
		return "0x" + hexValue[len(hexValue)-64:]
	}
	return "0x" + strings.Repeat("0", 64-len(hexValue)) + hexValue
}

func testERC20Log(block uint64, logIndex uint64, contract string, from string, to string, amount uint64) rpcLog {
	return testERC20LogWithData(block, logIndex, contract, from, to, testWordUint(amount), false)
}

func testERC20LogWithData(block uint64, logIndex uint64, contract string, from string, to string, data string, removed bool) rpcLog {
	return rpcLog{
		Address: normalizeAddress(contract),
		Topics: []string{
			TopicTransfer,
			testTopicAddress(from),
			testTopicAddress(to),
		},
		Data:             data,
		BlockNumber:      quantity(block),
		BlockHash:        fmt.Sprintf("0x%064x", block),
		TransactionHash:  fmt.Sprintf("0x%064x", 0x100000+block*100+logIndex),
		TransactionIndex: "0x0",
		LogIndex:         quantity(logIndex),
		Removed:          removed,
	}
}

func testERC721Log(block uint64, logIndex uint64, contract string, from string, to string, tokenID uint64) rpcLog {
	log := testERC20LogWithData(block, logIndex, contract, from, to, "0x", false)
	log.Topics = append(log.Topics, testWordUint(tokenID))
	return log
}

func testERC1155SingleLog(block uint64, logIndex uint64, contract string, from string, to string, tokenID uint64, amount uint64) rpcLog {
	return rpcLog{
		Address: normalizeAddress(contract),
		Topics: []string{
			TopicERC1155TransferSingle,
			testTopicAddress(testCarol),
			testTopicAddress(from),
			testTopicAddress(to),
		},
		Data:             strings.TrimPrefix(testWordUint(tokenID), "0x") + strings.TrimPrefix(testWordUint(amount), "0x"),
		BlockNumber:      quantity(block),
		BlockHash:        fmt.Sprintf("0x%064x", block),
		TransactionHash:  fmt.Sprintf("0x%064x", 0x200000+block*100+logIndex),
		TransactionIndex: "0x0",
		LogIndex:         quantity(logIndex),
	}
}

func testERC1155BatchLog(block uint64, logIndex uint64, contract string, from string, to string, ids []uint64, values []uint64) rpcLog {
	return rpcLog{
		Address: normalizeAddress(contract),
		Topics: []string{
			TopicERC1155TransferBatch,
			testTopicAddress(testCarol),
			testTopicAddress(from),
			testTopicAddress(to),
		},
		Data:             testBatchData(ids, values),
		BlockNumber:      quantity(block),
		BlockHash:        fmt.Sprintf("0x%064x", block),
		TransactionHash:  fmt.Sprintf("0x%064x", 0x300000+block*100+logIndex),
		TransactionIndex: "0x0",
		LogIndex:         quantity(logIndex),
	}
}

func testBatchData(ids []uint64, values []uint64) string {
	valueOffset := uint64(64 + (1+len(ids))*32)
	words := []string{testWordUint(64), testWordUint(valueOffset), testWordUint(uint64(len(ids)))}
	for _, id := range ids {
		words = append(words, testWordUint(id))
	}
	words = append(words, testWordUint(uint64(len(values))))
	for _, value := range values {
		words = append(words, testWordUint(value))
	}
	var builder strings.Builder
	builder.WriteString("0x")
	for _, word := range words {
		builder.WriteString(strings.TrimPrefix(word, "0x"))
	}
	return builder.String()
}

func mustRequestRange(t *testing.T, request rpcRequest) (uint64, uint64) {
	t.Helper()
	if len(request.Params) != 1 {
		t.Fatalf("params len = %d, want 1", len(request.Params))
	}
	params, ok := request.Params[0].(map[string]interface{})
	if !ok {
		t.Fatalf("params[0] = %T, want map", request.Params[0])
	}
	fromRaw, ok := params["fromBlock"].(string)
	if !ok {
		t.Fatalf("fromBlock = %T, want string", params["fromBlock"])
	}
	toRaw, ok := params["toBlock"].(string)
	if !ok {
		t.Fatalf("toBlock = %T, want string", params["toBlock"])
	}
	from, err := parseHexUint(fromRaw)
	if err != nil {
		t.Fatalf("parse fromBlock: %v", err)
	}
	to, err := parseHexUint(toRaw)
	if err != nil {
		t.Fatalf("parse toBlock: %v", err)
	}
	return from, to
}
