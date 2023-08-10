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
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

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

type IndexedBuffer struct {
	buffer *bytes.Buffer
	i      int
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

	ChunkSize       int64    `yaml:"chunkSize"`
	IncludeDirs     []string `yaml:"includeDirs"`
	ExcludePatterns []string `yaml:"excludePatterns"`
}

func main() {
	now := time.Now()
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

	tarReader, tarPipeWriter := io.Pipe()
	tarWriter := tar.NewWriter(tarPipeWriter)
	var tarLock sync.Mutex

	var wg sync.WaitGroup
	wg.Add(runtime.NumCPU())

	fileNameChan := make(chan struct {
		fileName string
		stat     fs.FileInfo
	}, runtime.NumCPU())

	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			for task := range fileNameChan {
				func() {
					fileName := task.fileName
					stat := task.stat

					log.Println("Opening file", fileName)

					file, err := os.Open(fileName)
					if err != nil {
						log.Println("Unable to open file", err)
						return
					}
					defer file.Close()

					compressedFile := new(bytes.Buffer)
					defer compressedFile.Reset()
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

					tarLock.Lock()
					defer tarLock.Unlock()

					log.Printf("Compressed %s, writing...\n", fileName)
					check0(tarWriter.WriteHeader(header))
					check(io.Copy(tarWriter, compressedFile))

					log.Printf("Wrote %s into archive\n", fileName)
				}()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Reading files")

		for _, includeDir := range config.IncludeDirs {
			log.Println("Looking through", includeDir)

			i := 0

			check0(filepath.Walk(includeDir, func(fileName string, stat fs.FileInfo, err error) error {
				if err != nil {
					return nil
				}

				if stat.IsDir() {
					return nil
				}

				if ignorer.MatchesPath(strings.ToLower(fileName)) {
					return nil
				}

				if i%1e4 == 0 {
					log.Println("Skipping over files...")
				}

				i++

				log.Println("Walking on file", fileName)
				fileNameChan <- struct {
					fileName string
					stat     fs.FileInfo
				}{fileName, stat}

				return nil
			}))

			log.Println("Finished walking")
			wg.Wait()
			close(fileNameChan)
		}

		tarWriter.Close()
	}()

	encryptedBufferChan := make(chan IndexedBuffer)
	defer close(encryptedBufferChan)
	unencryptedBufferChan := make(chan IndexedBuffer)
	defer close(unencryptedBufferChan)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for task := range unencryptedBufferChan {
			log.Println("Encrypting chunk")
			encryptedBuffer := new(bytes.Buffer)
			encryptWriter := check(openpgp.Encrypt(encryptedBuffer, entities, nil, nil, nil))
			check(io.Copy(encryptWriter, task.buffer))

			log.Println("Chunk ready, encrypting and uploading", task.i)
			encryptedBufferChan <- IndexedBuffer{encryptedBuffer, task.i}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Println("Compressing and sending")

		n := int64(math.MaxInt64)
		i := 0
		for n >= config.ChunkSize {
			unencryptedBuffer := new(bytes.Buffer)
			n = check(io.CopyN(unencryptedBuffer, tarReader, int64(config.ChunkSize)))
			log.Println("Chunk collected", n)
			unencryptedBufferChan <- IndexedBuffer{unencryptedBuffer, i}
			i++
		}

		close(encryptedBufferChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for task := range encryptedBufferChan {
			log.Println("Uploading chunk")
			info := check(minioClient.PutObject(
				context.Background(),
				config.S3.Bucket,
				fmt.Sprintf("archive %s/%d.tar.gz.gpg", now.Local().String(), task.i),
				task.buffer,
				int64(task.buffer.Len()),
				minio.PutObjectOptions{
					ConcurrentStreamParts: true,
					NumThreads:            uint(runtime.NumCPU()),
				},
			))

			log.Println("Chunk uploaded", info)
		}
	}()

	wg.Wait()
	log.Println("DONE")
}
