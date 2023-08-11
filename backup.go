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
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	progressbar "github.com/schollz/progressbar/v3"
	"golang.org/x/crypto/openpgp"
)

func Backup(config Config) {
	now := time.Now()

	tarReader, tarPipeWriter := io.Pipe()
	tarWriter := tar.NewWriter(tarPipeWriter)
	var tarLock sync.Mutex

	var wg sync.WaitGroup

	fileNameChan := make(chan struct {
		fileName string
		stat     fs.FileInfo
	}, runtime.NumCPU())

	var fileWg sync.WaitGroup
	fileWg.Add(runtime.NumCPU())

	wg.Add(1)
	go func() {
		defer wg.Done()

		defer fileWg.Wait()

		for i := 0; i < runtime.NumCPU(); i++ {
			go func() {
				defer fileWg.Done()
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
	}()

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

				if config.Ignorer.MatchesPath(strings.ToLower(fileName)) {
					return nil
				}

				fileNameChan <- struct {
					fileName string
					stat     fs.FileInfo
				}{fileName, stat}

				return nil
			}))
		}
		close(fileNameChan)
		fileWg.Wait()

		log.Println("------------\nFinished walking, closing tar file")
		check0(tarWriter.Flush())
		check0(tarWriter.Close())
		check0(tarPipeWriter.Close())
	}()

	encryptedBufferChan := make(chan IndexedBuffer)
	unencryptedBufferChan := make(chan IndexedBuffer)

	wg.Add(1)
	go func() {
		defer wg.Done()

		var encryptWg sync.WaitGroup
		encryptWg.Add(2)

		go func() {
			defer encryptWg.Done()
			for task := range unencryptedBufferChan {
				encryptedBuffer := bytes.NewBuffer(make([]byte, 0, task.buffer.Len()))
				encryptWriter := check(openpgp.Encrypt(encryptedBuffer, config.Entities, nil, nil, nil))
				check(io.Copy(encryptWriter, task.buffer))
				log.Printf("Encrypted chunk %d", task.i)
				encryptedBufferChan <- IndexedBuffer{encryptedBuffer, task.i}
			}
			close(encryptedBufferChan)
		}()

		go func() {
			defer encryptWg.Done()
			n := int64(math.MaxInt64)
			i := 0
			var err error
			for n >= config.ChunkSize {
				unencryptedBuffer := bytes.NewBuffer(make([]byte, 0, config.ChunkSize))
				log.Println("Collecting chunk", i)
				n, err = io.CopyN(unencryptedBuffer, tarReader, config.ChunkSize)

				if n > 0 {
					unencryptedBufferChan <- IndexedBuffer{unencryptedBuffer, i}
				}
				i++

				if err != nil {
					break
				}
			}
			close(unencryptedBufferChan)
		}()

	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var uploadWg sync.WaitGroup
		defer uploadWg.Wait()

		for task := range encryptedBufferChan {
			if config.DryRun {
				log.Println("DRYRUN uploading buffer", task.buffer.Len())
			} else {
				uploadWg.Add(1)
				go func() {
					defer uploadWg.Done()
					for {
						bar := progressbar.DefaultBytes(int64(task.buffer.Len()), fmt.Sprintf("Uploading %d", task.i))

						_, err := config.MinioClient.PutObject(
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

						bar.Close()

						if err == nil {
							break
						} else {
							log.Println("MINIO ERROR", err)
							time.Sleep(30 * time.Second)
						}
					}
				}()
			}
			runtime.GC()
		}
	}()

	wg.Wait()
	log.Println("DONE", time.Since(now))
}
