package ethereum

import (
	"log"

	"github.com/ethereum/go-ethereum/ethclient"
)

func NewEthereumClient(wsURL string) *ethclient.Client {
	client, err := ethclient.Dial(wsURL)
	if err != nil {
		log.Fatalf("Erro ao conectar no nó Ethereum: %v", err)
	}
	log.Println("✅ Conectado ao nó Geth por WebService!")
	return client
}
