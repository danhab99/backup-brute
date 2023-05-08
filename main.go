package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/crypto/openpgp"
)

func main() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	log.Println("Starting")

	s3access := flag.String("s3-access", "", "s3 access key")
	s3secret := flag.String("s3-secret", "", "s3 secret key")
	s3endpoint := flag.String("s3-endpoint", "", "s3 endpoint (no http)")
	s3region := flag.String("s3-region", "us-east-1", "s3 region")
	backupFile := flag.String("file", "/dev/sda", "file path of device to backup")
	privateKeyFile := flag.String("privatekey-file", "", "The file where the private key is stored for encryption")
	chunkSize := flag.Int("chunk-size", 1e6, "how big each chunk should be")

	flag.Parse()

	creds := credentials.NewStaticV4(*s3access, *s3secret, "")

	minioClient, err := minio.New(*s3endpoint, &minio.Options{
		Creds:  creds,
		Secure: true,
		Region: *s3region,
	})

	log.Println("Setup minio")

	disk, err := os.Open(*backupFile)
	if err != nil {
		panic(err)
	}
	defer disk.Close()
	log.Println("Opened disk")

	privateKey, err := os.Open(*privateKeyFile)
	if err != nil {
		panic(err)
	}
	defer privateKey.Close()
	log.Println("Opened private key")

	entities, err := openpgp.ReadArmoredKeyRing(privateKey)
	if err != nil {
		panic(err)
	}
	log.Println("Collected keys", entities)

	chunkChan := make(chan []byte)

	go func() {
		for {
			bits := make([]byte, *chunkSize)
			n, err := disk.Read(bits)
			if err != nil {
				panic(err)
			}
			log.Println("Emitting chunk", n, len(bits))
			chunkChan <- bits
			if n < *chunkSize {
				break
			}
		}
		close(chunkChan)
	}()

	zippedChunkChan := make(chan []byte)

	go func() {
		zipBuff := new(bytes.Buffer)
		for chunk := range chunkChan {
			log.Println("Writing compressed chunk")
			zipWriter := gzip.NewWriter(zipBuff)
			_, err := zipWriter.Write(chunk)
			if err != nil {
				panic(err)
			}
			log.Println("Compressing chunk complete, adding", zipBuff.Len())

			if zipBuff.Len() >= *chunkSize {
				log.Println("Compress buffer full, flusing")
				zippedChunkChan <- zipBuff.Bytes()
				zipBuff = new(bytes.Buffer)
			}
		}
		close(zippedChunkChan)
	}()

	encryptedChunkChan := make(chan []byte)

	go func() {
		for chunk := range zippedChunkChan {
			buff := new(bytes.Buffer)
			log.Println("Encrypting chunk", entities, buff.Len())
			writer, err := openpgp.Encrypt(buff, entities, nil, nil, nil)
			if err != nil {
				panic(err)
			}
			writer.Write(chunk)
			log.Println("Finished encrypting chunk")
			encryptedChunkChan <- buff.Bytes()
		}
		close(encryptedChunkChan)
	}()

	finished := make(chan struct{})

	go func() {
		now := time.Now()
		i := 0
		for chunk := range encryptedChunkChan {
			buff := new(bytes.Buffer)
			buff.Write(chunk)

			info, err := minioClient.PutObject(
				context.Background(),
				"danhabot-desktop-backups",
				fmt.Sprintf("%s-%d.gz.gpg", now, i),
				buff,
				int64(buff.Len()),
				minio.PutObjectOptions{},
			)

			if err != nil {
				panic(err)
			}
			log.Println("Finished uploading", i, info)
			i++
		}
		finished <- struct{}{}
		close(finished)
	}()

	<-finished
}
