package evm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDecodeERCTransfersAndEdgeCases(t *testing.T) {
	adapter := mustDecodeAdapter(t)

	mintEvents, err := adapter.decodeLog(testERC20Log(10, 0, testUSDC, testZeroAddr, testAlice, 100), "mock")
	if err != nil {
		t.Fatalf("decode mint: %v", err)
	}
	if mintEvents[0].From != testZeroAddr || mintEvents[0].To != testAlice {
		t.Fatalf("mint addresses = %s -> %s", mintEvents[0].From, mintEvents[0].To)
	}

	burnEvents, err := adapter.decodeLog(testERC20Log(11, 0, testUSDC, testAlice, testZeroAddr, 100), "mock")
	if err != nil {
		t.Fatalf("decode burn: %v", err)
	}
	if burnEvents[0].From != testAlice || burnEvents[0].To != testZeroAddr {
		t.Fatalf("burn addresses = %s -> %s", burnEvents[0].From, burnEvents[0].To)
	}

	erc721Events, err := adapter.decodeLog(testERC721Log(12, 0, testERC721, testAlice, testBob, 123), "mock")
	if err != nil {
		t.Fatalf("decode erc721: %v", err)
	}
	if erc721Events[0].TokenStandard != TokenStandardERC721 || erc721Events[0].TokenID != "123" || erc721Events[0].AmountRaw != "1" {
		t.Fatalf("erc721 event = %#v", erc721Events[0])
	}

	singleEvents, err := adapter.decodeLog(testERC1155SingleLog(13, 0, testERC1155, testAlice, testBob, 7, 42), "mock")
	if err != nil {
		t.Fatalf("decode erc1155 single: %v", err)
	}
	if singleEvents[0].TokenStandard != TokenStandardERC1155 || singleEvents[0].TokenID != "7" || singleEvents[0].AmountRaw != "42" {
		t.Fatalf("erc1155 single event = %#v", singleEvents[0])
	}

	batchEvents, err := adapter.decodeLog(testERC1155BatchLog(14, 0, testERC1155, testAlice, testBob, []uint64{1, 2}, []uint64{10, 20}), "mock")
	if err != nil {
		t.Fatalf("decode erc1155 batch: %v", err)
	}
	if len(batchEvents) != 2 || batchEvents[0].BatchIndex != 0 || batchEvents[1].BatchIndex != 1 || batchEvents[1].TokenID != "2" || batchEvents[1].AmountRaw != "20" {
		t.Fatalf("erc1155 batch events = %#v", batchEvents)
	}

	huge := "0x" + strings.Repeat("f", 64)
	unknownEvents, err := adapter.decodeLog(testERC20LogWithData(15, 0, testUnknown, testAlice, testBob, testWordHex(huge), false), "mock")
	if err != nil {
		t.Fatalf("decode unknown huge token: %v", err)
	}
	const maxUint256 = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	if unknownEvents[0].TokenStandard != TokenStandardERC20 || unknownEvents[0].TokenSymbol != "" || unknownEvents[0].AmountRaw != maxUint256 {
		t.Fatalf("unknown huge event = %#v", unknownEvents[0])
	}
}

func TestFetchRangeHandlesMalformedDuplicateAndRemovedLogs(t *testing.T) {
	good := testERC20Log(16, 0, testUSDC, testAlice, testBob, 10)
	duplicate := good
	malformed := testERC20LogWithData(17, 0, testUSDC, testAlice, testBob, "0x", false)
	removed := testERC20LogWithData(18, 0, testUSDC, testAlice, testBob, testWordUint(11), true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode RPC request: %v", err)
		}
		switch request.Method {
		case "eth_blockNumber":
			writeRPCResult(t, w, request.ID, "0x20")
		case "eth_getLogs":
			writeRPCResult(t, w, request.ID, []rpcLog{good, duplicate, malformed, removed})
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()

	adapter := mustAdapter(t, server.URL)
	events, err := adapter.FetchRange(context.Background(), 16, 18)
	if err != nil {
		t.Fatalf("FetchRange returned error: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("event count = %d, want duplicate removed and malformed retained", len(events))
	}
	if events[1].EventType != "decode_error" || events[1].DecodeError == "" || events[1].DiscardReason != "malformed_log" {
		t.Fatalf("malformed event = %#v", events[1])
	}
	if !events[2].Reorged || events[2].FinalityStatus != FinalityStatusReorged {
		t.Fatalf("removed event = %#v", events[2])
	}
}

func mustDecodeAdapter(t *testing.T) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(Config{
		ChainName:               "ethereum",
		Network:                 "mainnet",
		ChainID:                 1,
		FinalityDepth:           2,
		ReorgWindow:             8,
		MaxBlockRange:           100,
		InitialBlockRange:       100,
		MinBlockRange:           1,
		MaxGetLogsRetries:       1,
		GetLogsRetryBackoff:     time.Nanosecond,
		GetLogsRetryMaxBackoff:  time.Nanosecond,
		ReconnectInitialBackoff: time.Nanosecond,
		ReconnectMaxBackoff:     time.Nanosecond,
		HeartbeatInterval:       time.Second,
		RPCEndpoints: []EndpointConfig{{
			Name: "mock-rpc",
			URL:  "http://127.0.0.1/mock",
		}},
		WatchedContracts: []WatchedContract{
			{Address: testUSDC, TokenStandard: TokenStandardERC20, TokenSymbol: "USDC"},
			{Address: testERC721, TokenStandard: TokenStandardERC721, TokenSymbol: "NFT"},
			{Address: testERC1155, TokenStandard: TokenStandardERC1155, TokenSymbol: "ITEM"},
		},
	})
	if err != nil {
		t.Fatalf("NewAdapter returned error: %v", err)
	}
	return adapter
}
