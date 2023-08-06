package main

import (
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/crypto/openpgp"
	"gopkg.in/yaml.v3"
)

func check[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func check0(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("Starting")

	type Config struct {
		S3 struct {
			Access   string `yaml:"access"`
			Secret   string `yaml:"secret"`
			Region   string `yaml:"region"`
			Endpoint string `yaml:"endpoint"`
			Bucket   string `yaml:"bucket"`
		} `yaml:"s3"`

		GPG struct {
			PrivateKeyFile string `yaml:"privateKeyFile"`
		} `yaml:"gpg"`

		ChunkSize  int    `yaml:"chunkSize"`
		BackupFile string `yaml:"backupFile"`
	}

	var config Config

	for _, filepath := range []string{
		"/etc/backup.yaml",
		path.Join(check(os.UserHomeDir()), "backup.yaml"),
		path.Join(check(os.UserConfigDir()), "backup.yaml"),
		path.Join(check(os.Getwd()), "backup.yaml"),
	} {
		raw, err := os.ReadFile(filepath)
		if err != nil {
			continue
		}
		check0(yaml.Unmarshal(raw, &config))
		log.Printf("Configs %s: %+v", filepath, config)
	}

	creds := credentials.NewStaticV4(config.S3.Access, config.S3.Secret, "")

	minioClient := check(minio.New(config.S3.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: true,
		Region: config.S3.Region,
	}))

	log.Println("Setup minio")

	disk := check(os.Open(config.BackupFile))
	defer disk.Close()
	log.Println("Opened " + config.BackupFile)

	privateKey := check(os.Open(config.GPG.PrivateKeyFile))
	defer privateKey.Close()
	log.Println("Opened private key")

	entities := check(openpgp.ReadArmoredKeyRing(privateKey))
	log.Println("Collected keys", entities)

	zippedReader, zippedWriter := io.Pipe()
	zipWrite := zlib.NewWriter(zippedWriter)

	go func() { check(io.Copy(zipWrite, disk)) }()

	now := time.Now()
	i := 0

	for {
		start := time.Now()
		encryptedReader, encryptedWriter := io.Pipe()
		encryptWriter, err := openpgp.Encrypt(encryptedWriter, entities, nil, nil, nil)
		if err != nil {
			panic(err)
		}

		log.Printf("Encrypting chunk %d\n", i)
		check(io.CopyN(encryptWriter, zippedReader, int64(config.ChunkSize)))
		// go func() { check(io.CopyN(encryptWriter, zippedReader, int64(config.ChunkSize))) }()

		log.Println("Encryption complete, uploading")
		info := check(minioClient.PutObject(
			context.Background(),
			"danhabot-desktop-backups",
			fmt.Sprintf("%s-%d.gz.gpg", url.QueryEscape(now.String()), i),
			encryptedReader,
			-1,
			minio.PutObjectOptions{
				ConcurrentStreamParts: true,
				NumThreads:            uint(runtime.NumCPU()),
			},
		))

		log.Printf("Upload successfully took %s: %v", time.Since(start), info)
		log.Println("--------------------")

		i++

		if info.Size < int64(config.ChunkSize) {
			break
		}
	}

}
