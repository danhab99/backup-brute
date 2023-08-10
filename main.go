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
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	ignore "github.com/sabhiram/go-gitignore"
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

	ChunkSize       int      `yaml:"chunkSize"`
	IncludeDirs     []string `yaml:"includeDirs"`
	ExcludePatterns []string `yaml:"excludePatterns"`
}

func main() {
	// log.SetFlags(log.Ldate | log.Ltime)
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	log.Println("Starting")

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

	log.Printf("Config: %+v\n ", config)

	ignorer := ignore.CompileIgnoreLines(config.ExcludePatterns...)

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

	tarReader, tarPipeWriter := io.Pipe()
	tarWriter := tar.NewWriter(tarPipeWriter)
	var tarLock sync.Mutex

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		log.Println("Reading files")

		for _, includeDir := range config.IncludeDirs {
			log.Println("Looking through", includeDir)

			pool := workerpool.New(runtime.NumCPU())
			defer pool.StopWait()

			check0(filepath.Walk(includeDir, func(fileName string, stat fs.FileInfo, err error) error {
				if err != nil {
					panic(err)
				}

				if stat.IsDir() {
					return nil
				}

				if ignorer.MatchesPath(strings.ToLower(fileName)) {
					return nil
				}

				pool.Submit(func() {
					log.Println("Opening file", fileName)

					file, err := os.Open(fileName)
					if err != nil {
						log.Println("Unable to open file", err)
						return
					}

					compressedFile := new(bytes.Buffer)
					gzipWriter := gzip.NewWriter(compressedFile)
					_, err = io.Copy(gzipWriter, file)
					if err != nil {
						log.Println("Unable to read file", err)
						return
					}

					header := &tar.Header{
						Name:    fileName,
						Size:    int64(compressedFile.Len()),
						Mode:    int64(stat.Mode()),
						ModTime: stat.ModTime(),
					}

					log.Printf("Compressed %s, writing... %+v\n", fileName, header)

					defer tarLock.Unlock()
					tarLock.Lock()
					check0(tarWriter.WriteHeader(header))
					check(io.Copy(tarWriter, compressedFile))
				})

				return nil
			}))

		}
	}()

	go func() {
		defer wg.Done()

		log.Println("Compressing and sending")

		n := int64(math.MaxInt64)
		var i int
		for n >= int64(config.ChunkSize) {
			unencryptedBuffer := new(bytes.Buffer)
			n = check(io.CopyN(unencryptedBuffer, tarReader, int64(config.ChunkSize)))
			log.Println("Chunk ready, encrypting and uploading", i)

			encryptedBuffer := new(bytes.Buffer)
			encryptWriter := check(openpgp.Encrypt(encryptedBuffer, entities, nil, nil, nil))
			check(io.Copy(encryptWriter, encryptedBuffer))

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
			i++
		}
	}()

	wg.Wait()
	log.Println("DONE")
}
