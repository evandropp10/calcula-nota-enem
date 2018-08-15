package main

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type Aluno struct {
	NU_INSCRICAO    string
	NU_NOTA_CH      float64
	NU_NOTA_CN      float64
	NU_NOTA_LC      float64
	NU_NOTA_MT      float64
	NU_NOTA_REDACAO int
	NOTA_FINAL      float64
}

type AlunoJson struct {
	NU_INSCRICAO string
	NOTA_FINAL   float64
}

func main() {
	ctx := context.Background()
	projectID := "codenation-data-science"
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Println(err)
	}

	//criaResultado(client, ctx, projectID)
	criaBGTableResultado(client, ctx)
}

func criaResultado(client *bigquery.Client, ctx context.Context, projectID string) {

	//  1 -- Leitura da base original do ENEM, para cálculo da nota final

	q := client.Query(`
	       SELECT NU_INSCRICAO, NU_NOTA_CH, NU_NOTA_CN, NU_NOTA_LC, NU_NOTA_MT, NU_NOTA_REDACAO
	       FROM` + "`codenation-data-science.Dados.enem2016`" +
		`WHERE NU_NOTA_CH > 0
	   			AND NU_NOTA_CN > 0
	   			AND NU_NOTA_LC > 0
	   			AND NU_NOTA_MT > 0
	   			AND NU_NOTA_REDACAO > 0
	   `)
	it, err := q.Read(ctx)
	if err != nil {
		log.Println(err)
	}

	var notas [][]string
	var nota []string

	for {
		var aluno Aluno
		var alunoJson AlunoJson

		err1 := it.Next(&aluno)
		if err1 == iterator.Done {
			break
		}
		if err1 != nil {
			log.Println(err)
		}

		aluno.NOTA_FINAL = ((aluno.NU_NOTA_CH * 1) + (aluno.NU_NOTA_CN * 2) + (aluno.NU_NOTA_LC * 1.5) + (aluno.NU_NOTA_MT * 3) + (float64(aluno.NU_NOTA_REDACAO) * 3)) / 10.5

		aluno.NOTA_FINAL, err = strconv.ParseFloat(strconv.FormatFloat(float64(aluno.NOTA_FINAL), 'f', 1, 64), 64)

		alunoJson.NU_INSCRICAO = aluno.NU_INSCRICAO
		alunoJson.NOTA_FINAL = aluno.NOTA_FINAL

		nota = nil
		nota = append(nota, alunoJson.NU_INSCRICAO)
		nota = append(nota, strconv.FormatFloat(alunoJson.NOTA_FINAL, 'f', -1, 64))
		notas = append(notas, nota)

		// 1 ##

		//log.Println(alunoJson)

	}
	//log.Println(notas)

	// coloca em csv

	file, err2 := os.Create("notasfinais.csv")
	checkError("Cannot create file", err2)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err3 := writer.WriteAll(notas) // calls Flush internally
	checkError("Cannot write csv", err3)

	/*
		uploader := t.Uploader()
		if err = uploader.Put(ctx, alunosJson.answer); err != nil {
			log.Println(err)
		}
	*/

	enviaCsvStorage(ctx, projectID)

}

func enviaCsvStorage(ctx context.Context, projectID string) {

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	bucket := "dados_codenation"
	object := "notasfinais.csv"

	if err := writeStorage(client, bucket, object); err != nil {
		log.Fatalf("Cannot write object: %v", err)
	}
}

func writeStorage(client *storage.Client, bucket, object string) error {
	ctx := context.Background()
	// [START upload_file]

	file, err := os.Open("notasfinais.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err = io.Copy(wc, file); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	// [END upload_file]
	return nil

}

// Cria tabela com as notas finais no Dataset Dados do Big Query
func criaBGTableResultado(client *bigquery.Client, ctx context.Context) {

	datasetDados := client.Dataset("Dados")
	t := datasetDados.Table("enem2016_notafinal")

	schema1 := bigquery.Schema{
		{Name: "NU_INSCRICAO", Required: true, Type: bigquery.StringFieldType},
		{Name: "NOTA_FINAL", Repeated: false, Type: bigquery.FloatFieldType},
	}

	if err := t.Create(ctx, &bigquery.TableMetadata{Schema: schema1}); err != nil {
		log.Println(err)
	}

	loadDataBQ(t, ctx, schema1)

}

func loadDataBQ(t *bigquery.Table, ctx context.Context, schema1 bigquery.Schema) {

	gcsRef := bigquery.NewGCSReference("gs://dados_codenation/notasfinais.csv")
	gcsRef.AllowJaggedRows = true
	gcsRef.MaxBadRecords = 5
	//gcsRef.Schema = schema1
	// TODO: set other options on the GCSReference.
	//loader := datasetDados.Table("my_table").LoaderFrom(gcsRef)
	loader := t.LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateNever
	// TODO: set other options on the Loader.
	job, err := loader.Run(ctx)
	log.Println(1)
	if err != nil {
		log.Println(err)
	}
	log.Println(2)
	status, err := job.Wait(ctx)
	if err != nil {
		log.Println(err)
	}
	if status.Err() != nil {
		log.Println(status.Errors)
	}

}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
