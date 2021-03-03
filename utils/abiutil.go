package utils

import (
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
		abiUtil.Abis[contractAbi.Address] = contractAbi
	}

	return &abiUtil, nil
}