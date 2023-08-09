package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
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
	// log.SetFlags(log.Ldate | log.Ltime)
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
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

		ChunkSize int `yaml:"chunkSize"`

		includeDirs     []string `yaml:"includeDir"`
		excludePatterns []string `yaml:"excludePatterns"`
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

	privateKey := check(os.Open(config.GPG.PrivateKeyFile))
	defer privateKey.Close()
	log.Println("Opened private key")

	entities := check(openpgp.ReadArmoredKeyRing(privateKey))
	log.Println("Collected keys", entities)

	now := time.Now()

	tarBuff := new(bytes.Buffer)
	tarWriter := tar.NewWriter(tarBuff)

	var tarLock sync.Locker
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		log.Println("Reading files")

		for _, includeDir := range config.includeDirs {
			log.Println("Looking through", includeDir)
			check0(filepath.Walk(includeDir, func(fileName string, stat fs.FileInfo, err error) error {
				for _, pattern := range config.excludePatterns {
					if check(filepath.Match(pattern, fileName)) {
						log.Println("Skipping file", fileName)
						return nil
					}
				}

				if err != nil {
					panic(err)
				}

				go func() {
					defer wg.Done()
					wg.Add(1)
					log.Println("Opening file", fileName)
					file := check(os.Open(fileName))
					compressedFile := new(bytes.Buffer)
					gzipWriter := gzip.NewWriter(compressedFile)
					check(io.Copy(gzipWriter, file))

					defer tarLock.Unlock()
					tarLock.Lock()

					header := &tar.Header{
						Name:    fileName,
						Size:    stat.Size(),
						Mode:    int64(stat.Mode()),
						ModTime: stat.ModTime(),
					}

					log.Printf("Compressed %s, writing... %+v", fileName, header)
					check0(tarWriter.WriteHeader(header))
					check(io.Copy(tarWriter, compressedFile))
				}()

				return nil
			}))
		}
	}()

	go func() {
		defer wg.Done()

		log.Println("Compressing and sending")

		var n int64
		var i int
		for n >= int64(config.ChunkSize) {
			unencryptedBuffer := new(bytes.Buffer)
			n = check(io.CopyN(unencryptedBuffer, tarBuff, int64(config.ChunkSize)))
			log.Println("Chunk ready, encrypting and uploading", i)

			go func(unencryptedBuffer *bytes.Buffer, i int) {
				defer wg.Done()
				wg.Add(1)

				encryptedBuffer := new(bytes.Buffer)
				encryptWriter := check(openpgp.Encrypt(encryptedBuffer, entities, nil, nil, nil))
				go func() { check(io.Copy(encryptWriter, encryptedBuffer)) }()

				info := check(minioClient.PutObject(
					context.Background(),
					"danhabot-desktop-backups",
					fmt.Sprintf("%s-%d.gz.gpg", url.QueryEscape(now.String()), i),
					encryptedBuffer,
					int64(encryptedBuffer.Len()),
					minio.PutObjectOptions{
						ConcurrentStreamParts: true,
						NumThreads:            uint(runtime.NumCPU()),
					},
				))

				log.Println("Chunk uploaded", info)
			}(unencryptedBuffer, i)
			i++
		}
	}()

	wg.Wait()
}
