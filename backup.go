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

	fileNameChan := make(chan NamedBuffer)

	go func() {
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
		close(fileNameChan)
	}()

	fileBufferChan := chanWorker[NamedBuffer, NamedBuffer](fileNameChan, 2, func(in NamedBuffer) NamedBuffer {
		return NamedBuffer{
			filepath: in.filepath,
			info:     in.info,
			buffer:   bytes.NewBuffer(check(os.ReadFile(in.filepath))),
		}
	})

	tarReader, tarPipeWriter := io.Pipe()
	tarWriter := tar.NewWriter(tarPipeWriter)

	go func() {
		defer func() {
			check0(tarWriter.Flush())
			check0(tarWriter.Close())
			check0(tarPipeWriter.Close())
		}()

		for namedbuffer := range fileBufferChan {
			header := &tar.Header{
				Name:    namedbuffer.filepath,
				Size:    int64(namedbuffer.buffer.Len()),
				Mode:    int64(namedbuffer.info.Mode()),
				ModTime: namedbuffer.info.ModTime(),
			}
			check0(tarWriter.WriteHeader(header))
			check(io.Copy(tarWriter, namedbuffer.buffer))
		}
	}()

	tarChunkChan := makeChunks(tarReader, int64(config.chunkSize))
	recipient := check(age.ParseX25519Identity(config.Config.Age.Private))

	uploadTaskChunk := make(chan func(), runtime.NumCPU())

	var wg sync.WaitGroup
	wg.Add(config.chunkCount)
	wg.Add(runtime.NumCPU())

	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			for upload := range uploadTaskChunk {
				upload()
			}
		}()
	}

	for i := 0; i < int(config.chunkCount); i++ {
		go func() {
			defer wg.Done()
			for chunk := range tarChunkChan {
				reader, writer := io.Pipe()

				var wg sync.WaitGroup
				wg.Add(2)

				go func() {
					defer wg.Done()

					check(config.MinioClient.PutObject(
						context.Background(),
						config.Config.S3.Bucket,
						fmt.Sprintf("%s/%d", now.Format(time.RFC3339), chunk.i),
						reader,
						-1,
						minio.PutObjectOptions{
							ConcurrentStreamParts: true,
							NumThreads:            uint(runtime.NumCPU()),
						},
					))
				}()

				go func() {
					defer wg.Done()
					gzipWriter := check(gzip.NewWriterLevel(writer, gzip.BestCompression))
					encryptWriter := check(age.Encrypt(gzipWriter, recipient.Recipient()))

					hasMore := make(chan bool)
					uploadedBytes := uint64(0)

					work := func() {
						n, err := io.CopyN(encryptWriter, chunk.buffer, int64(config.upload))
						uploadedBytes += uint64(n)
						log.Printf("Uploading chunk %d %f%%\n", chunk.i, float64(uploadedBytes)/float64(config.chunkSize)*100)
						if err == nil {
							hasMore <- n > 0
						} else {
							hasMore <- false
						}
					}

					uploadTaskChunk <- work
					for <-hasMore {
						uploadTaskChunk <- work
					}

					close(hasMore)
					check0(encryptWriter.Close())
					check0(gzipWriter.Flush())
					check0(gzipWriter.Close())
					check0(writer.Close())
				}()

				wg.Wait()
			}
		}()
	}

	wg.Wait()

	log.Println("DONE", time.Since(now))
}
