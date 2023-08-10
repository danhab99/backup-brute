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
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	ignore "github.com/sabhiram/go-gitignore"
	progressbar "github.com/schollz/progressbar/v3"
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

func AllFieldsDefined(v interface{}) bool {
	value := reflect.ValueOf(v)
	if value.Kind() != reflect.Struct {
		return false
	}

	numFields := value.NumField()
	for i := 0; i < numFields; i++ {
		fieldValue := value.Field(i)
		if reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldValue.Type()).Interface()) {
			return false
		}
	}

	return true
}

type ReaderWithLength interface {
	Read(p []byte) (n int, err error)
	Len() int
}

func copyProgress(writer io.Writer, reader ReaderWithLength, label string) (int64, error) {
	bar := progressbar.DefaultBytes(
		int64(reader.Len()),
		label,
	)
	defer bar.Close()
	return io.Copy(io.MultiWriter(writer, bar), reader)
}

func copyProgressN(writer io.Writer, reader io.Reader, n int64, label string) (int64, error) {
	bar := progressbar.DefaultBytes(n, label)
	defer bar.Close()
	return io.CopyN(io.MultiWriter(writer, bar), reader, n)
}

func main() {
	now := time.Now()

	progressbar.OptionSetElapsedTime(true)

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
		if AllFieldsDefined(config) {
			break
		}
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

					content, err := os.ReadFile(fileName)
					if err != nil {
						log.Println("Unable to open file", err)
						return
					}

					log.Printf("Compressing file %s...\n", fileName)

					compressedFile := new(bytes.Buffer)
					gzipWriter := gzip.NewWriter(compressedFile)
					_, err = io.Copy(gzipWriter, bytes.NewBuffer(content))
					if err != nil {
						log.Println("Unable to compress file", err)
						return
					}
					log.Printf("Compressed file %s, writing to tar file\n", fileName)

					if compressedFile.Len() > 0 {
						header := &tar.Header{
							Name:    fileName,
							Size:    int64(compressedFile.Len()),
							Mode:    int64(stat.Mode()),
							ModTime: stat.ModTime(),
						}

						defer tarLock.Unlock()
						tarLock.Lock()

						check0(tarWriter.WriteHeader(header))
						check(io.Copy(tarWriter, compressedFile))
						log.Printf("Finished writing %s to tar\n", fileName)
					}
				}()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for _, includeDir := range config.IncludeDirs {
			check0(filepath.Walk(includeDir, func(fileName string, stat fs.FileInfo, err error) error {
				if err != nil {
					return nil
				}

				if stat.IsDir() {
					return nil
				}

				if !stat.Mode().IsRegular() {
					return nil
				}

				if stat.Size() <= 0 {
					return nil
				}

				if ignorer.MatchesPath(strings.ToLower(fileName)) {
					return nil
				}

				fileNameChan <- struct {
					fileName string
					stat     fs.FileInfo
				}{fileName, stat}

				return nil
			}))

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
			encryptedBuffer := bytes.NewBuffer(make([]byte, 0, task.buffer.Len()))
			encryptWriter := check(openpgp.Encrypt(encryptedBuffer, entities, nil, nil, nil))
			check(io.Copy(encryptWriter, task.buffer))
			fmt.Sprintf("Encrypted chunk %d", task.i)
			encryptedBufferChan <- IndexedBuffer{encryptedBuffer, task.i}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		n := int64(math.MaxInt64)
		i := 0
		for n >= config.ChunkSize {
			unencryptedBuffer := bytes.NewBuffer(make([]byte, 0, config.ChunkSize))
			n = check(io.CopyN(unencryptedBuffer, tarReader, config.ChunkSize))
			log.Println("Collecting chunk", i)
			unencryptedBufferChan <- IndexedBuffer{unencryptedBuffer, i}
			i++
		}

		close(encryptedBufferChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for task := range encryptedBufferChan {
			for {
				bar := progressbar.DefaultBytes(int64(task.buffer.Len()), fmt.Sprintf("Uploading %d", task.i))

				_, err := minioClient.PutObject(
					context.Background(),
					config.S3.Bucket,
					fmt.Sprintf("archive %s/%d.tar.gz.gpg", now.Local().String(), task.i),
					task.buffer,
					int64(task.buffer.Len()),
					minio.PutObjectOptions{
						ConcurrentStreamParts: true,
						NumThreads:            uint(runtime.NumCPU()),
						Progress:              bar,
					},
				)

				if err == nil {
					break
				} else {
					log.Println("MINIO ERROR", err)
					time.Sleep(30 * time.Second)
				}

				bar.Close()
			}
			runtime.GC()
		}
	}()

	wg.Wait()
	log.Println("DONE", time.Since(now))
}
