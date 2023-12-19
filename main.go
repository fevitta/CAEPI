package main

import (
	"archive/zip"
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/godror/godror"
	"github.com/joho/godotenv"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"

	"github.com/jlaffaye/ftp"
)

func main() {
	fmt.Println("Iniciando...")

	//FUNCIONANDO, COMENTADO PARA NAO BAIXAR TODA VEZ
	// Find .env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Arquivo de configuracao .env nao encontrado: %s", err)
	}

	err = downloadFTP()
	if err != nil {
		log.Fatalf("Erro ao baixar arquivo: %s", err)
	}

	err = UnZip(os.Getenv("ARQUIVO"), "dados", "/")
	if err != nil {
		log.Fatalf("Erro ao descompactar: %s", err)
	}

	os.Rename("dados/tgg_export_caepi.txt", "dados/dados.csv")

	// Converte de WIN1252 -> UTF8
	err = DecodeFile("dados/dados.csv", "dados/dados_utf8.csv")
	if err != nil {
		log.Fatalf("problem reading from file: %v", err)
	}

	listaCAEPI, err := converteCSVparaCAEPI("dados/dados_utf8.csv")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Registros para processar:", len(listaCAEPI))

	err = limparTabelaOracle("SGM.CAEPI")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " - Inicio INSERT")

	err = atualizaDadosCAEPI(listaCAEPI)
	//err = atualizaDadosCAEPITras(listaCAEPI)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " - Fim INSERT")
}

func insereLogRobo(roboID, situacaoID, registros int, err error) error {
	//TODO inserir log do robo nas tabelas, GERENCIAL.EXECUCAO e GERENCIAL.LOG_ROBO. ID_ROBO = 55
	fmt.Println(roboID, situacaoID, registros)
	//situacaoID
	// 1	EXECUTANDO
	// 2	FINALIZADO COM SUCESSO
	// 3	FINALIZADO COM ALERTA
	// 4	FINALIZADO COM ERRO
	return nil
}

func left(s string, maxSize int) string {
	l := len(s)
	if l < maxSize {
		return s[:l]
	} else {
		return s[:maxSize]
	}
}

func converteCSVparaCAEPI(caminhoCSV string) ([]CAEPIRecord, error) {
	fmt.Println("Abrindo arquivo:", caminhoCSV)
	f, err := os.Open(caminhoCSV)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// read csv values using csv.Reader
	fmt.Println("Convertendo arquivo em CSV")
	csvReader := csv.NewReader(f)
	csvReader.Comma = '|'
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	var listaCAEPI []CAEPIRecord
	for i, line := range data {
		//limitar regs para teste
		// if i == 2000 {
		// 	break
		// }
		if i > 0 { // pular cabecalho
			var rec CAEPIRecord
			for j, field := range line {
				switch j {
				// Campos estao limitados ao tamanho maximo das colunas no banco de dados
				case 0:
					rec.NRREGISTROCA = left(field, 50)
				case 1:
					rec.DATAVALIDADE = left(field, 10)
				case 2:
					rec.SITUACAO = left(field, 20)
				case 3:
					rec.NRPROCESSO = left(field, 20)
				case 4:
					rec.CNPJ = left(field, 14)
				case 5:
					rec.RAZAOSOCIAL = left(field, 200)
				case 6:
					rec.NATUREZA = left(field, 20)
				case 7:
					rec.NOMEEQUIPAMENTO = left(field, 200)
				case 8:
					rec.DESCRICAOEQUIPAMENTO = left(field, 3000)
				case 9:
					rec.MARCACA = left(field, 100)
				case 10:
					rec.REFERENCIA = left(field, 200)
				case 11:
					rec.COR = left(field, 200)
				case 12:
					rec.APROVADOPARALAUDO = left(field, 3000)
				case 13:
					rec.RESTRICAOLAUDO = left(field, 3000)
				case 14:
					rec.OBSERVACAOANALISELAUDO = left(field, 3000)
				case 15:
					rec.CNPJLABORATORIO = left(field, 14)
				case 16:
					rec.RAZAOSOCIALLABORATORIO = left(field, 200)
				case 17:
					rec.NRLAUDO = left(field, 100)
				case 18:
					rec.NORMA = left(field, 50)
				default:
				}
			}
			listaCAEPI = append(listaCAEPI, rec)
		}
		// print the array
		//fmt.Printf("%+v\n", listaCAEPI)
	}
	return listaCAEPI, nil
}

func DecodeFile(sourceFile, destFile string) error {
	// TODO - Melhoria, passar o charmap como parametro na funcao
	file, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer file.Close()

	err = os.Remove(destFile)

	newFile, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer newFile.Close()

	decodingReader := transform.NewReader(file, charmap.Windows1252.NewDecoder())

	scanner := bufio.NewScanner(decodingReader)

	for scanner.Scan() {
		//lines = append(lines, scanner.Text())
		newFile.Write([]byte(scanner.Text() + "\n"))
	}
	return scanner.Err()
}

func limparTabelaOracle(tabela string) error {
	fmt.Println("Conectando ao banco:", os.Getenv("ORACLE_CONNECTION"))
	user := os.Getenv("ORACLE_USER")
	password := os.Getenv("ORACLE_PASS")
	connectString := os.Getenv("ORACLE_CONNECTION")

	db, err := sql.Open("godror", fmt.Sprintf("%s/%s@%s", user, password, connectString))

	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer db.Close()
	fmt.Println("Conectado. Stats:", db.Stats())

	fmt.Println("Limpando tabela", tabela)
	_, err = db.Exec("truncate table " + tabela)
	if err != nil {
		fmt.Println("Erro limpando tabela...")
		fmt.Println(err)
		return nil
	}

	return nil
}

func atualizaDadosCAEPITras(listaCAEPI []CAEPIRecord) error {
	//TODO Concluir insert usando transaction
	user := os.Getenv("ORACLE_USER")
	password := os.Getenv("ORACLE_PASS")
	connectString := os.Getenv("ORACLE_CONNECTION")

	db, err := sql.Open("godror", fmt.Sprintf("%s/%s@%s", user, password, connectString))
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer db.Close()

	// Teste a conexão
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Println("Conectado. Stats:", db.Stats())

	// Preparar a declaração de inserção em massa
	stmt, err := db.Prepare("INSERT INTO SGM.CAEPI (ID, NRREGISTROCA) VALUES (SGM.SEQ_CAEPI.NEXTVAL,:1)")
	// "INSERT INTO SGM.CAEPI " +
	// 	"(ID, NRREGISTROCA, DATAVALIDADE, SITUACAO, NRPROCESSO, CNPJ, RAZAOSOCIAL, NATUREZA, NOMEEQUIPAMENTO, DESCRICAOEQUIPAMENTO, MARCACA, REFERENCIA, COR, APROVADOPARALAUDO, RESTRICAOLAUDO, OBSERVACAOANALISELAUDO, CNPJLABORATORIO, RAZAOSOCIALLABORATORIO, NRLAUDO, NORMA)" +
	// 	"VALUES (SGM.SEQ_CAEPI.NEXTVAL,:1,TO_DATE(:2,'DD/MM/YYYY'),:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)")

	if err != nil {
		log.Fatal(err)
		return err
	}
	defer stmt.Close()

	// Iniciar uma transação
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer tx.Rollback()

	limiteCommit := 10
	contador := 0

	// Executar as inserções em massa
	for _, itemCAEPI := range listaCAEPI {
		_, err := tx.Stmt(stmt).Exec(itemCAEPI.NRREGISTROCA)
		// itemCAEPI.NRREGISTROCA, itemCAEPI.DATAVALIDADE, itemCAEPI.SITUACAO, itemCAEPI.NRPROCESSO, itemCAEPI.CNPJ,
		// itemCAEPI.RAZAOSOCIAL, itemCAEPI.NATUREZA, itemCAEPI.NOMEEQUIPAMENTO, itemCAEPI.DESCRICAOEQUIPAMENTO, itemCAEPI.MARCACA,
		// itemCAEPI.REFERENCIA, itemCAEPI.COR, itemCAEPI.APROVADOPARALAUDO, itemCAEPI.RESTRICAOLAUDO, itemCAEPI.OBSERVACAOANALISELAUDO,
		// itemCAEPI.CNPJLABORATORIO, itemCAEPI.RAZAOSOCIALLABORATORIO, itemCAEPI.NRLAUDO, itemCAEPI.NORMA)
		if err != nil {
			log.Fatal(err)
		}
		contador++
		// Realizar o commit a cada 1000 registros
		if contador%limiteCommit == 0 {
			// Commit da transação
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Commit bem-sucedido para %d registros\n", contador)

			// Iniciar uma nova transação
			tx, err = db.Begin()
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// Commit da transação
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Inserção em massa concluída com sucesso!")

	return nil
}

func atualizaDadosCAEPI2(listaCAEPI []CAEPIRecord) error {
	port := 1521
	connStr := go_ora.BuildUrl("server", port, "service_name", "username", "password", nil)
	conn, err := sql.Open("oracle", connStr)
	// check for error
	err = conn.Ping()
	// check for error
	return nil
}

func atualizaDadosCAEPI(listaCAEPI []CAEPIRecord) error {
	//https://godror.github.io/godror/doc/contents.html
	//https://developer.oracle.com/learn/technical-articles/way-to-go-on-oci-article4
	fmt.Println("Conectando ao banco:", os.Getenv("ORACLE_CONNECTION"))
	user := os.Getenv("ORACLE_USER")
	password := os.Getenv("ORACLE_PASS")
	connectString := os.Getenv("ORACLE_CONNECTION")

	db, err := sql.Open("godror", fmt.Sprintf("%s/%s@%s", user, password, connectString))

	if err != nil {
		fmt.Println(err)
		return err
	}
	defer db.Close()

	fmt.Println("Conectado", db.Stats())

	limiteCommit := 500
	contador := 0
	inicio := time.Now()

	for _, itemCAEPI := range listaCAEPI {
		_, err := db.Exec(
			"INSERT INTO SGM.CAEPI "+
				"(ID, NRREGISTROCA, DATAVALIDADE, SITUACAO, NRPROCESSO, CNPJ, RAZAOSOCIAL, NATUREZA, NOMEEQUIPAMENTO, DESCRICAOEQUIPAMENTO, MARCACA, REFERENCIA, COR, APROVADOPARALAUDO, RESTRICAOLAUDO, OBSERVACAOANALISELAUDO, CNPJLABORATORIO, RAZAOSOCIALLABORATORIO, NRLAUDO, NORMA)"+
				"VALUES (SGM.SEQ_CAEPI.NEXTVAL,:1,TO_DATE(:2,'DD/MM/YYYY'),:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)",
			itemCAEPI.NRREGISTROCA, itemCAEPI.DATAVALIDADE, itemCAEPI.SITUACAO, itemCAEPI.NRPROCESSO, itemCAEPI.CNPJ,
			itemCAEPI.RAZAOSOCIAL, itemCAEPI.NATUREZA, itemCAEPI.NOMEEQUIPAMENTO, itemCAEPI.DESCRICAOEQUIPAMENTO, itemCAEPI.MARCACA,
			itemCAEPI.REFERENCIA, itemCAEPI.COR, itemCAEPI.APROVADOPARALAUDO, itemCAEPI.RESTRICAOLAUDO, itemCAEPI.OBSERVACAOANALISELAUDO,
			itemCAEPI.CNPJLABORATORIO, itemCAEPI.RAZAOSOCIALLABORATORIO, itemCAEPI.NRLAUDO, itemCAEPI.NORMA)

		if err != nil {
			fmt.Println("Erro inserindo:", itemCAEPI.NRREGISTROCA, itemCAEPI.DATAVALIDADE, itemCAEPI.SITUACAO, itemCAEPI.NRPROCESSO, itemCAEPI.CNPJ,
				itemCAEPI.RAZAOSOCIAL, itemCAEPI.NATUREZA, itemCAEPI.NOMEEQUIPAMENTO, itemCAEPI.DESCRICAOEQUIPAMENTO, itemCAEPI.MARCACA,
				itemCAEPI.REFERENCIA, itemCAEPI.COR, itemCAEPI.APROVADOPARALAUDO, itemCAEPI.RESTRICAOLAUDO, itemCAEPI.OBSERVACAOANALISELAUDO,
				itemCAEPI.CNPJLABORATORIO, itemCAEPI.RAZAOSOCIALLABORATORIO, itemCAEPI.NRLAUDO, itemCAEPI.NORMA)
			fmt.Println(err)
			return err
		}
		defer db.Close()
		contador++
		if contador%limiteCommit == 0 {
			tempoDecorrido := time.Since(inicio)
			fmt.Printf("%d registros inseridos em %s\n", contador, tempoDecorrido)
		}
	}

	tempoDecorrido := time.Since(inicio)
	fmt.Printf("Fim dos inserts. %d registros inseridos em %s\n", contador, tempoDecorrido)

	return nil
}

func downloadFTP() error {
	//Fazer o download apenas quando a data de alteração dos arquivos forem diferente (FTP x Local)

	arquivo := os.Getenv("ARQUIVO")
	caminho := os.Getenv("CAMINHO")
	ftp_host := os.Getenv("FTP_HOST")
	ftp_port := os.Getenv("FTP_PORT")
	ftp_user := os.Getenv("FTP_USER")
	ftp_pass := os.Getenv("FTP_PASS")

	fmt.Println("Conectando ao FTP:", ftp_host)

	c, err := ftp.Dial(ftp_host+":"+ftp_port, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Println("Efetuando login...")

	err = c.Login(ftp_user, ftp_pass)
	if err != nil {
		log.Fatal(err)
		return err
	}

	UltAlteracao, err := c.GetTime(caminho + arquivo)
	if err != nil {
		fmt.Println("Erro ao obter data da ultima alteracao do arquivo no FTP:", arquivo, err)
	}

	// Obter informações sobre o arquivo
	dataModificacao := time.Now()
	info, err := os.Stat(arquivo)
	if err != nil {
		log.Println(err)
	} else {
		// Obter a data de modificação do arquivo
		dataModificacao = info.ModTime()
	}
	if dataModificacao.Local().Compare(UltAlteracao) != 0 {
		//Lista arquivos na raiz do FTP
		//fmt.Println(c.NameList("/"))
		//fmt.Println(c.NameList("/portal/fiscalizacao/seguranca-e-saude-no-trabalho/caepi/"))
		fmt.Println("Baixando arquivo... ()", arquivo)
		r, err := c.Retr(caminho + arquivo)
		if err != nil {
			panic(err)
		}
		defer r.Close()

		buf, err := io.ReadAll(r)

		os.WriteFile(arquivo, buf, 0644)
		//alterar data de alteracao do arquivo do arquivo baixado
		os.Chtimes(arquivo, UltAlteracao.Local(), UltAlteracao.Local())

		fmt.Println("Download finalizado.")
	} else {
		fmt.Println("Arquivo do FTP está igual ao local, ignorando etapa de download...")
	}

	return nil
}

func UnZip(src, dest, ignoreDir string) error {

	reader, err := zip.OpenReader(src)
	if err != nil {
		return err
	}

	defer reader.Close()

	for _, f := range reader.File {
		rc, err := f.Open()
		if err != nil {
			log.Fatal(err)
			return err
		}

		defer rc.Close()

		fpath := filepath.Join(dest, f.Name)

		if lastIndex := strings.LastIndex(fpath, ignoreDir); lastIndex == -1 {
			if f.FileInfo().IsDir() {
				err = os.MkdirAll(fpath, 0755)
				if err != nil {
					log.Fatal(err)
					return err
				}
			} else {
				var fdir string
				if lastIndex := strings.LastIndex(fpath, string(os.PathSeparator)); lastIndex > -1 {
					fdir = fpath[:lastIndex]
				}

				if len(f.Name) < 150 {

					err = os.MkdirAll(fdir, 0755)
					if err != nil {
						log.Fatal(err)
						return err
					}

					fi, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
					if err != nil {
						log.Fatal(err)
						return err
					}

					defer fi.Close()

					_, err = io.Copy(fi, rc)
					if err != nil {
						log.Fatal(err)
						return err
					}
				}
			}
		}
	}

	return nil
}

type CAEPIRecord struct {
	ID                     int
	NRREGISTROCA           string
	DATAVALIDADE           string
	SITUACAO               string
	NRPROCESSO             string
	CNPJ                   string
	RAZAOSOCIAL            string
	NATUREZA               string
	NOMEEQUIPAMENTO        string
	DESCRICAOEQUIPAMENTO   string
	MARCACA                string
	REFERENCIA             string
	COR                    string
	APROVADOPARALAUDO      string
	RESTRICAOLAUDO         string
	OBSERVACAOANALISELAUDO string
	CNPJLABORATORIO        string
	RAZAOSOCIALLABORATORIO string
	NRLAUDO                string
	NORMA                  string
}
