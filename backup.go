package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"filippo.io/age"
	"github.com/minio/minio-go/v7"
)

func Backup(config *BackupConfig) {
	now := time.Now()
	recipient := check(age.ParseX25519Identity(config.Config.Age.Private))

	sizeOfChunksBeingMade := uint64(runtime.NumCPU()) * config.chunkSize
	sizeOfChunksBeingUploaded := config.maxRam - sizeOfChunksBeingMade
	numberOfChunksToUpload := int(sizeOfChunksBeingUploaded / config.chunkSize)

	fileNameChan := make(chan NamedBuffer)
	var wg sync.WaitGroup
	wg.Add(2)
	wg.Add(runtime.NumCPU())

	go func() {
		defer wg.Done()
		defer close(fileNameChan)

		for _, includeDir := range config.Config.IncludeDirs {
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

				fileNameChan <- NamedBuffer{fileName, stat, nil}

				return nil
			}))
		}
	}()

	index := 0
	var indexLock sync.Mutex

	uploadBufferChan := make(chan IndexedBuffer)

	go func() {
		defer wg.Done()

		var wg sync.WaitGroup
		wg.Add(runtime.NumCPU())

		for i := 0; i < runtime.NumCPU()-1; i++ {
			go func(i int) {
				defer wg.Done()

				l := log.Default()
				l.SetPrefix(fmt.Sprintf("Worker%d: ", i))

				running := true
				for running {
					indexLock.Lock()
					thisIndex := index
					index++
					indexLock.Unlock()

					buff := IndexedBuffer{
						buffer: make([]byte, 0, config.chunkSize),
						i:      thisIndex,
					}

					buffWriter := NewByteArrayWriter(&buff.buffer)
					encryptWriter := check(age.Encrypt(buffWriter, recipient.Recipient()))
					gzipWriter := check(gzip.NewWriterLevel(encryptWriter, gzip.BestCompression))
					tarWriter := tar.NewWriter(gzipWriter)

					l.Println("Working on chunk")

					for len(buff.buffer) <= int(config.chunkSize) {
						namedbuffer, ok := <-fileNameChan
						if !ok {
							running = false
							break
						}

						if namedbuffer.info.Size()+int64(len(buff.buffer)) > int64(config.chunkSize) {
							fileNameChan <- namedbuffer
							break
						}

						header := &tar.Header{
							Name:    namedbuffer.filepath,
							Size:    int64(namedbuffer.info.Size()),
							Mode:    int64(namedbuffer.info.Mode()),
							ModTime: namedbuffer.info.ModTime(),
						}
						check0(tarWriter.WriteHeader(header))

						l.Println("Reading file", namedbuffer.filepath)

						tarWriter.Write(check(os.ReadFile(namedbuffer.filepath)))

						check0(tarWriter.Flush())
						check0(gzipWriter.Flush())
					}

					check0(tarWriter.Close())
					check0(gzipWriter.Close())
					check0(encryptWriter.Close())

					l.Println("Sending buffer", buff.i)
					uploadBufferChan <- buff
				}
			}(i)
		}

		wg.Wait()
		close(uploadBufferChan)
	}()

	for i := 0; i < numberOfChunksToUpload; i++ {
		go func() {
			defer wg.Done()

			for task := range uploadBufferChan {
				for {
					runtime.GC()

					buffer := NewByteArrayReader(task.buffer)
					l := int64(len(task.buffer))

					log.Println("Uploading chunk", task.i)
					_, err := config.MinioClient.PutObject(
						context.Background(),
						config.Config.S3.Bucket,
						fmt.Sprintf("%s/%d", now.Format(time.RFC3339), task.i),
						buffer,
						l,
						minio.PutObjectOptions{},
					)

					if err == nil {
						log.Println("Uploaded chunk", task.i)
						break
					} else {
						log.Println("MINIO ERROR", err)
						time.Sleep(30 * time.Second)
					}
				}
			}
		}()
	}

	wg.Wait()
	log.Println("DONE", time.Since(now))
}
