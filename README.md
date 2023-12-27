# CAEPI
Obter os dados do site do FTP do CAEPI e converter o arquivo para UTF8

https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/seguranca-e-saude-no-trabalho/equipamentos-de-protecao-individual-epi/passo-a-passo-importar-dados-do-caepi.pdf

A função DownloadFTP() baixa, descompacta e converte o arquivo tgg_export_caepi.zip para UTF8.

A função ConverteCSVparaCAEPI() le o arquivo .csv e o converte para struct CaepiRecord

## Instalação
```bash
go get github.com/fevitta/ftp_caepi@latest
```

## Exemplo
```go
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/fevitta/ftp_caepi"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("Iniciando...")

    err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Arquivo de configuracao .env nao encontrado: %s", err)
	}

	err = ftp_caepi.DownloadFTP()
	if err != nil {
		log.Fatalf("Erro ao baixar arquivo: %s", err)
	}

	listaCAEPI, err := ftp_caepi.ConverteCSVparaCAEPI("dados/dados_utf8.csv")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Registros processados:", len(listaCAEPI))
}    
```

## .env (Opcional)
Crie um arquivo .env com os parametros se necessário alterar os valores padrões
```
ARQUIVO="tgg_export_caepi.zip"
CAMINHO="portal/fiscalizacao/seguranca-e-saude-no-trabalho/caepi/"
FTP_HOST="ftp.mtps.gov.br"
FTP_PORT=21
FTP_USER="anonymous"
FTP_PASS="anonymous"
```