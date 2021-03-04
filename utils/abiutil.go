package utils

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

type AbiUtil struct {
	Abis map[string]*ContractAbi
}

func NewAbi(networkID uint32) (*AbiUtil, error) {
	var contractAbis []*ContractAbi
	switch networkID {
	case 5:
		contractAbis = ContractAbis5
	case 1:
		contractAbis = ContractAbis1
	default:
		return nil, fmt.Errorf("invalid network id")
	}

	abiUtil := AbiUtil{}

	abiUtil.Abis = make(map[string]*ContractAbi)

	for _, contractAbi := range contractAbis {
		abiTool, err := abi.JSON(strings.NewReader(contractAbi.Abis))
		if err != nil {
			return nil, err
		}
		contractAbi.AbiTool = abiTool

		for _, ev := range abiTool.Events {
			evm := abi.NewMethod(ev.Name, ev.RawName, abi.Function, "", false, false, ev.Inputs, abi.Arguments{})
			contractAbi.EventFunc = append(contractAbi.EventFunc, &evm)
		}

		abiUtil.Abis[contractAbi.Address] = contractAbi
	}

	return &abiUtil, nil
}

func (a *ContractAbi) FindEvent(sigdata []byte) (*abi.Method, error) {
	for _, method := range a.EventFunc {
		if bytes.Equal(method.ID, sigdata[:4]) {
			return method, nil
		}
	}
	return nil, fmt.Errorf("no method with id: %#x", sigdata[:4])
}