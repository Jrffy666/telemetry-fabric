package evm

import (
	"fmt"
	"strings"
	"time"
)

const (
	TopicTransfer              = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	TopicERC1155TransferSingle = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
	TopicERC1155TransferBatch  = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)

func (a *Adapter) decodeLog(log rpcLog, sourceRPC string) ([]NormalizedChainEvent, error) {
	if len(log.Topics) == 0 {
		return nil, nil
	}

	signature := normalizeHex(log.Topics[0])
	switch signature {
	case TopicTransfer:
		event, err := a.decodeTransfer(log, sourceRPC)
		if err != nil {
			return nil, err
		}
		return []NormalizedChainEvent{event}, nil
	case TopicERC1155TransferSingle:
		event, err := a.decodeERC1155TransferSingle(log, sourceRPC)
		if err != nil {
			return nil, err
		}
		return []NormalizedChainEvent{event}, nil
	case TopicERC1155TransferBatch:
		return a.decodeERC1155TransferBatch(log, sourceRPC)
	default:
		return nil, nil
	}
}

func (a *Adapter) decodeTransfer(log rpcLog, sourceRPC string) (NormalizedChainEvent, error) {
	if len(log.Topics) < 3 {
		return NormalizedChainEvent{}, fmt.Errorf("transfer log has %d topics, want at least 3", len(log.Topics))
	}

	from, err := addressFromTopic(log.Topics[1])
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	to, err := addressFromTopic(log.Topics[2])
	if err != nil {
		return NormalizedChainEvent{}, err
	}

	contract := normalizeAddress(log.Address)
	watched := a.watched[contract]
	standard := watched.TokenStandard
	if standard == "" || standard == TokenStandardUnspecified {
		if len(log.Topics) >= 4 {
			standard = TokenStandardERC721
		} else {
			standard = TokenStandardERC20
		}
	}

	amountRaw := "1"
	tokenID := ""
	if standard == TokenStandardERC721 {
		if len(log.Topics) < 4 {
			return NormalizedChainEvent{}, fmt.Errorf("erc721 transfer log has %d topics, want 4", len(log.Topics))
		}
		word := log.Topics[3]
		tokenID, err = bigIntStringFromWord(word)
		if err != nil {
			return NormalizedChainEvent{}, err
		}
	} else {
		word, err := wordAt(log.Data, 0)
		if err != nil {
			return NormalizedChainEvent{}, err
		}
		amountRaw, err = bigIntStringFromWord(word)
		if err != nil {
			return NormalizedChainEvent{}, err
		}
		standard = TokenStandardERC20
	}

	event, err := a.baseEvent(log, sourceRPC)
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	event.EventType = "transfer"
	event.From = from
	event.To = to
	event.Contract = contract
	event.TokenAddress = contract
	event.TokenSymbol = watched.TokenSymbol
	event.TokenStandard = standard
	event.AmountRaw = amountRaw
	event.TokenID = tokenID
	return event, nil
}

func (a *Adapter) decodeERC1155TransferSingle(log rpcLog, sourceRPC string) (NormalizedChainEvent, error) {
	if len(log.Topics) < 4 {
		return NormalizedChainEvent{}, fmt.Errorf("erc1155 TransferSingle log has %d topics, want 4", len(log.Topics))
	}

	from, err := addressFromTopic(log.Topics[2])
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	to, err := addressFromTopic(log.Topics[3])
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	idWord, err := wordAt(log.Data, 0)
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	valueWord, err := wordAt(log.Data, 1)
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	tokenID, err := bigIntStringFromWord(idWord)
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	amountRaw, err := bigIntStringFromWord(valueWord)
	if err != nil {
		return NormalizedChainEvent{}, err
	}

	contract := normalizeAddress(log.Address)
	watched := a.watched[contract]
	event, err := a.baseEvent(log, sourceRPC)
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	event.EventType = "transfer"
	event.From = from
	event.To = to
	event.Contract = contract
	event.TokenAddress = contract
	event.TokenSymbol = watched.TokenSymbol
	event.TokenStandard = TokenStandardERC1155
	event.AmountRaw = amountRaw
	event.TokenID = tokenID
	return event, nil
}

func (a *Adapter) decodeERC1155TransferBatch(log rpcLog, sourceRPC string) ([]NormalizedChainEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("erc1155 TransferBatch log has %d topics, want 4", len(log.Topics))
	}

	from, err := addressFromTopic(log.Topics[2])
	if err != nil {
		return nil, err
	}
	to, err := addressFromTopic(log.Topics[3])
	if err != nil {
		return nil, err
	}

	ids, values, err := decodeUint256Arrays(log.Data)
	if err != nil {
		return nil, err
	}
	if len(ids) != len(values) {
		return nil, fmt.Errorf("erc1155 TransferBatch ids length %d != values length %d", len(ids), len(values))
	}

	contract := normalizeAddress(log.Address)
	watched := a.watched[contract]
	events := make([]NormalizedChainEvent, 0, len(ids))
	for i := range ids {
		event, err := a.baseEvent(log, sourceRPC)
		if err != nil {
			return nil, err
		}
		event.EventType = "transfer"
		event.From = from
		event.To = to
		event.Contract = contract
		event.TokenAddress = contract
		event.TokenSymbol = watched.TokenSymbol
		event.TokenStandard = TokenStandardERC1155
		event.AmountRaw = values[i]
		event.TokenID = ids[i]
		event.BatchIndex = uint32(i)
		events = append(events, event)
	}
	return events, nil
}

func (a *Adapter) baseEvent(log rpcLog, sourceRPC string) (NormalizedChainEvent, error) {
	blockNumber, err := parseHexUint(log.BlockNumber)
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	txIndex, err := parseHexUint(log.TransactionIndex)
	if err != nil {
		return NormalizedChainEvent{}, err
	}
	logIndex, err := parseHexUint(log.LogIndex)
	if err != nil {
		return NormalizedChainEvent{}, err
	}

	return NormalizedChainEvent{
		Chain:          a.cfg.ChainName,
		Network:        a.cfg.Network,
		ChainID:        a.cfg.ChainID,
		BlockNumber:    blockNumber,
		BlockHash:      normalizeHex(log.BlockHash),
		TxHash:         normalizeHex(log.TransactionHash),
		TxIndex:        txIndex,
		LogIndex:       logIndex,
		FinalityStatus: FinalityStatusPending,
		SourceRPC:      sourceRPC,
		Reorged:        log.Removed,
		ObservedAt:     time.Now().UTC(),
	}, nil
}

func decodeUint256Arrays(data string) ([]string, []string, error) {
	firstOffsetWord, err := wordAt(data, 0)
	if err != nil {
		return nil, nil, err
	}
	secondOffsetWord, err := wordAt(data, 1)
	if err != nil {
		return nil, nil, err
	}
	firstIndex, err := offsetWordToIndex(firstOffsetWord)
	if err != nil {
		return nil, nil, err
	}
	secondIndex, err := offsetWordToIndex(secondOffsetWord)
	if err != nil {
		return nil, nil, err
	}

	ids, err := decodeUint256ArrayAt(data, firstIndex)
	if err != nil {
		return nil, nil, err
	}
	values, err := decodeUint256ArrayAt(data, secondIndex)
	if err != nil {
		return nil, nil, err
	}
	return ids, values, nil
}

func decodeUint256ArrayAt(data string, startWord int) ([]string, error) {
	lengthWord, err := wordAt(data, startWord)
	if err != nil {
		return nil, err
	}
	length, err := parseHexUint(lengthWord)
	if err != nil {
		return nil, err
	}
	if length > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("ABI array length %d exceeds platform int", length)
	}
	values := make([]string, 0, int(length))
	for i := uint64(0); i < length; i++ {
		word, err := wordAt(data, startWord+1+int(i))
		if err != nil {
			return nil, err
		}
		value, err := bigIntStringFromWord(word)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func hasTopicPrefix(topic, prefix string) bool {
	return strings.HasPrefix(strings.TrimPrefix(normalizeHex(topic), "0x"), strings.TrimPrefix(normalizeHex(prefix), "0x"))
}
