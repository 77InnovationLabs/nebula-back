package ethereum

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	mykafka "eth-listener/internal/kafka"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	kafkago "github.com/segmentio/kafka-go"
)

// decodeEventLog decodifica um evento baseado na ABI do contrato
func decodeEventLog(contractABI abi.ABI, vLog types.Log) (string, map[string]interface{}, error) {
	for name, event := range contractABI.Events {
		if vLog.Topics[0] == event.ID {
			decodedData := make(map[string]interface{})

			err := contractABI.UnpackIntoMap(decodedData, name, vLog.Data)
			if err != nil {
				return name, nil, fmt.Errorf("❌ Erro ao decodificar dados do evento: %v", err)
			}

			for i, input := range event.Inputs {
				if input.Indexed {
					decodedData[input.Name] = vLog.Topics[i+1].Hex()
				}
			}

			return name, decodedData, nil
		}
	}

	return hex.EncodeToString(vLog.Topics[0].Bytes()), nil, fmt.Errorf("❌ Evento desconhecido")
}

// getContractABI busca a ABI do contrato na API do Etherscan
func GetContractABI(contractAddress string) (abi.ABI, error) {
	apiKey := os.Getenv("API_KEY_ETHERSCAN")
	if apiKey == "" {
		return abi.ABI{}, fmt.Errorf("❌ API_KEY_ETHERSCAN não definida no ambiente")
	}
	url := fmt.Sprintf("https://api-sepolia.etherscan.io/api?module=contract&action=getabi&address=%s&apikey=%s", contractAddress, apiKey)

	resp, err := http.Get(url)
	if err != nil {
		return abi.ABI{}, fmt.Errorf("❌ Erro ao acessar API do Etherscan: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return abi.ABI{}, fmt.Errorf("❌ Erro ao ler resposta da API: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return abi.ABI{}, fmt.Errorf("❌ Erro ao parsear resposta da API: %v", err)
	}

	if result["status"] != "1" {
		return abi.ABI{}, fmt.Errorf("❌ ABI não encontrada para o contrato %s", contractAddress)
	}

	contractABI, err := abi.JSON(strings.NewReader(result["result"].(string)))
	if err != nil {
		return abi.ABI{}, fmt.Errorf("❌ Erro ao carregar ABI: %v", err)
	}

	fmt.Println("✅ ABI do contrato carregada com sucesso!")
	return contractABI, nil
}

// Buscar eventos passados e retorna o último bloco processado
func BuscarEventosPassados(client *ethclient.Client, contractAddress common.Address, contractABI abi.ABI, writer *kafkago.Writer) *big.Int {
	fmt.Println("🔍 Buscando eventos passados...")

	fromBlock := big.NewInt(7361255)
	batchSize := big.NewInt(50000)

	header, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Fatalf("❌ Erro ao obter último bloco: %v", err)
	}
	latestBlock := header.Number

	var lastProcessedBlock *big.Int

	for fromBlock.Cmp(latestBlock) < 0 {
		toBlock := new(big.Int).Add(fromBlock, batchSize)
		if toBlock.Cmp(latestBlock) > 0 {
			toBlock = latestBlock
		}

		query := ethereum.FilterQuery{
			FromBlock: fromBlock,
			ToBlock:   toBlock,
			Addresses: []common.Address{contractAddress},
		}

		logs, err := client.FilterLogs(context.Background(), query)
		if err != nil {
			log.Printf("❌ Erro ao buscar eventos (%d - %d): %v\n", fromBlock, toBlock, err)
			break
		}

		for _, vLog := range logs {
			processarEvento(client, contractABI, vLog, writer)
			lastProcessedBlock = new(big.Int).SetUint64(vLog.BlockNumber)
		}

		fromBlock.Add(fromBlock, batchSize)
	}

	if lastProcessedBlock == nil {
		lastProcessedBlock = latestBlock
	}

	return lastProcessedBlock
}

// ListenEvents monitora eventos do contrato via WebSocket em tempo real
func ListenEvents(client *ethclient.Client, contractAddress common.Address, contractABI abi.ABI, lastProcessedBlock *big.Int, writer *kafkago.Writer) {
	logsChan := make(chan types.Log)

	query := ethereum.FilterQuery{
		FromBlock: lastProcessedBlock,
		Addresses: []common.Address{contractAddress},
	}

	sub, err := client.SubscribeFilterLogs(context.Background(), query, logsChan)
	if err != nil {
		log.Fatalf("❌ Erro ao assinar eventos do contrato: %v", err)
	}

	fmt.Println("🎧 Ouvindo eventos novos em tempo real...")

	for {
		select {
		case err := <-sub.Err():
			log.Fatalf("❌ Erro na assinatura de eventos: %v", err)
		case vLog := <-logsChan:
			processarEvento(client, contractABI, vLog, writer)
		}
	}
}

// processarEvento extrai, exibe e envia evento ao Kafka
func processarEvento(client *ethclient.Client, contractABI abi.ABI, vLog types.Log, writer *kafkago.Writer) {
	eventJSON := extrairDadosEvento(client, contractABI, vLog)
	displayEventDetails(eventJSON)
	mykafka.SendEvent(writer, vLog.TxHash.Hex(), string(eventJSON))
}

// extrairDadosEvento processa os detalhes do evento e retorna um JSON
func extrairDadosEvento(client *ethclient.Client, contractABI abi.ABI, vLog types.Log) []byte {
	eventName, decodedData, err := decodeEventLog(contractABI, vLog)
	if err != nil {
		log.Printf("❌ Erro ao decodificar evento: %v\n", err)
		return []byte(`{}`)
	}

	blockNumber := vLog.BlockNumber
	timestamp := buscaTimestampPorBloco(client, blockNumber)

	// Buscar transação completa
	tx, isPending, err := client.TransactionByHash(context.Background(), vLog.TxHash)
	if err != nil {
		log.Printf("❌ Erro ao obter transação %s: %v\n", vLog.TxHash.Hex(), err)
		return []byte(`{}`)
	}

	// Definir Gas Price
	gasPrice := "N/A"
	var gasPriceBig *big.Int
	if tx.GasPrice() != nil {
		gasPriceBig = tx.GasPrice()
		gasPrice = gasPriceBig.String()
	}

	// Buscar recibo da transação para obter Gas Used e calcular a taxa
	taxaTransacao := "N/A"
	if !isPending {
		receipt, err := client.TransactionReceipt(context.Background(), vLog.TxHash)
		if err == nil {
			// Converter GasUsed (uint64) para big.Int
			gasUsedBig := new(big.Int).SetUint64(receipt.GasUsed)

			// Calcular taxa de transação: GasUsed * GasPrice
			if gasPriceBig != nil {
				taxaTransacao = new(big.Int).Mul(gasUsedBig, gasPriceBig).String()
			}
		} else {
			log.Printf("❌ Erro ao obter recibo da transação %s: %v\n", vLog.TxHash.Hex(), err)
		}
	}

	// Criar estrutura JSON do evento
	eventData := map[string]interface{}{
		"Evento":        eventName,
		"Dados":         decodedData,
		"Timestamp":     timestamp,
		"TxHash":        vLog.TxHash.Hex(),
		"Bloco":         blockNumber,
		"GasPrice":      gasPrice,
		"TaxaTransacao": taxaTransacao,
	}

	eventJSON, _ := json.Marshal(eventData)
	return eventJSON
}

// buscaTimestampPorBloco obtém o timestamp do bloco
func buscaTimestampPorBloco(client *ethclient.Client, blockNumber uint64) string {
	block, err := client.BlockByNumber(context.Background(), big.NewInt(int64(blockNumber)))
	if err != nil {
		return "N/A"
	}
	return time.Unix(int64(block.Time()), 0).Format(time.RFC3339)
}

// displayEventDetails exibe evento no console com formatação estruturada
func displayEventDetails(eventJSON []byte) {
	var eventData map[string]interface{}

	// Converter JSON para um mapa estruturado
	err := json.Unmarshal(eventJSON, &eventData)
	if err != nil {
		fmt.Printf("❌ Erro ao processar JSON do evento: %v\n", err)
		return
	}

	// Extrair campos do evento
	evento := eventData["Evento"]
	dados, _ := json.MarshalIndent(eventData["Dados"], "", "  ")
	timestamp := eventData["Timestamp"]
	txHash := eventData["TxHash"]

	// Garantir que o bloco seja impresso como inteiro e não em notação científica
	bloco := int64(0)
	if blockValue, ok := eventData["Bloco"].(float64); ok {
		bloco = int64(blockValue)
	}

	// Tratar Gas Price e Taxa da Transação corretamente
	gasPrice := "N/A"
	if gp, ok := eventData["GasPrice"].(string); ok {
		gasPrice = gp
	}

	taxaTransacao := "N/A"
	if txFee, ok := eventData["TaxaTransacao"].(string); ok {
		taxaTransacao = txFee
	}

	// Imprimir no formato correto
	fmt.Printf(`
-----------------------------------
📌 Evento: %s
📦 Dados: %s
📅 Timestamp: %s
🔗 Tx Hash: %s
🛑 Bloco: %d
⛽ Gas Price: %s
💰 Taxa da Transação: %s
-----------------------------------
`, evento, string(dados), timestamp, txHash, bloco, gasPrice, taxaTransacao)
}
