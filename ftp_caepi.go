package ftp_caepi

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"

	"github.com/jlaffaye/ftp"
)

func left(s string, maxSize int) string {
	l := len(s)
	if l < maxSize {
		return s[:l]
	} else {
		return s[:maxSize]
	}
}

// Converte cada linha do arquivo .csv para uma struct
// O arquivo possui inumeras falhas de layout, por isso defini alguns limites para os campos texto
func ConverteCSVparaCAEPI(caminhoCSV string) ([]CaepiRecord, error) {
	fmt.Println("Abrindo arquivo:", caminhoCSV)
	f, err := os.Open(caminhoCSV)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Converter o codigo para utilizar o csvReader.Read() ao invés do ReadAll()
	fmt.Println("Carregando CSV e validando os campos...")
	csvReader := csv.NewReader(f)
	csvReader.Comma = '|'
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1

	data, err := csvReader.ReadAll()
	fmt.Println("Linhas no CSV:", len(data))
	if err != nil {
		log.Fatal(err)
	}

	var listaCAEPI []CaepiRecord
	for i, line := range data {
		//limitar regs para teste
		// if i == 500 {
		// 	break
		// }
		if i > 0 { // pular cabecalho
			var rec CaepiRecord
			for j, field := range line {
				switch j {
				// O arquivo .csv possui inumeras linhas irregulares, ajustei um limite para cada campo
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
					fmt.Println("Excedido numero de colunas esperado. Linha:", i, "Coluna:", j, "Conteudo:", left(field, 20), "Len:", len(field))
				}
			}
			listaCAEPI = append(listaCAEPI, rec)
		}
		// print the array
		//fmt.Printf("%+v\n", listaCAEPI)
	}
	return listaCAEPI, nil
}

func decodeFile(sourceFile, destFile string) error {
	// TODO - Melhoria, passar o charmap como parametro na funcao
	file, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_ = os.Remove(destFile)

	newFile, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer newFile.Close()

	decodingReader := transform.NewReader(file, charmap.Windows1252.NewDecoder())

	scanner := bufio.NewScanner(decodingReader)

	for scanner.Scan() {
		// O arquivo original possui várias aberturas de string com " sem o seu devido par.
		// Por isso estou removendo todas as " do arquivo
		newFile.Write([]byte(strings.Replace(scanner.Text(), "\"", "", -1) + "\n"))
	}
	return scanner.Err()
}

// Baixa o arquivo tgg_export_caepi.zip do FTP do CAEPI
// https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/seguranca-e-saude-no-trabalho/equipamentos-de-protecao-individual-epi/passo-a-passo-importar-dados-do-caepi.pdf
// Após baixar o arquivo, descompacta e converte para UTF8
// Todas as " são removidas do arquivo, muitas delas não possuem fechamento e o csvReader se perder
// O arquivo só será baixado se o arquivo local for diferente (Verificado pela data de modificação)
// Os arquivos serão gerados na pasta /dados
func DownloadFTP() error {
	arquivo := os.Getenv("ARQUIVO")
	if arquivo == "" {
		arquivo = "tgg_export_caepi.zip"
	}

	caminho := os.Getenv("CAMINHO")
	if caminho == "" {
		caminho = "portal/fiscalizacao/seguranca-e-saude-no-trabalho/caepi/"
	}

	ftp_host := os.Getenv("FTP_HOST")
	if ftp_host == "" {
		ftp_host = "ftp.mtps.gov.br"
	}
	ftp_port := os.Getenv("FTP_PORT")
	if ftp_port == "" {
		ftp_port = "21"
	}
	ftp_user := os.Getenv("FTP_USER")
	if ftp_user == "" {
		ftp_user = "anonymous"
	}
	ftp_pass := os.Getenv("FTP_PASS")
	if ftp_pass == "" {
		ftp_pass = "anonymous"
	}

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
		fmt.Println("Erro ao obter data da ultima alteracao do arquivo no FTP:", caminho+arquivo, err)
		UltAlteracao = time.Now()
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
		if err != nil {
			log.Fatalf("Erro ao ler arquivo: %s", err)
		}

		os.WriteFile(arquivo, buf, 0644)
		//alterar data de alteracao do arquivo do arquivo baixado
		os.Chtimes(arquivo, UltAlteracao.Local(), UltAlteracao.Local())

		fmt.Println("Download finalizado.")

		err = unzip(arquivo, "dados", "/")
		if err != nil {
			log.Fatalf("Erro ao descompactar: %s", err)
		}

		os.Rename("dados/tgg_export_caepi.txt", "dados/dados.csv")

		// Converte de WIN1252 -> UTF8
		err = decodeFile("dados/dados.csv", "dados/dados_utf8.csv")
		if err != nil {
			log.Fatalf("Erro ao converter arquivo para UTF: %v", err)
		}
	} else {
		fmt.Println("Arquivo do FTP está igual ao local, ignorando etapa de download...")
	}

	return nil
}

func unzip(src, dest, ignoreDir string) error {

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

type CaepiRecord struct {
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
