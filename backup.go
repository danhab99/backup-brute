package main

import (
	"archive/tar"
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
	"github.com/klauspost/pgzip"
	"github.com/minio/minio-go/v7"
)

func Backup(config *BackupConfig) {
	now := time.Now()
	pool := NewBufferPool()
	recipient := check(age.ParseX25519Identity(config.Config.Age.Private))

	compressedReader, compressedWriter := io.Pipe()
	compressedChunks := makeChunks(compressedReader, &pool, int64(config.chunkSize))

	compressWriter := pgzip.NewWriter(compressedWriter)
	tarWriter := tar.NewWriter(compressWriter)

	compressedAndEncryptedChunks := make(chan IndexedBuffer, 1)
	var compresserWg sync.WaitGroup
	compresserWg.Add(runtime.NumCPU())

	go func() {
		compresserWg.Wait()
		close(compressedAndEncryptedChunks)
	}()

	for gid := 0; gid < runtime.NumCPU(); gid++ {
		go func() {
			defer compresserWg.Done()
			for compressedChunk := range compressedChunks {
				encryptedBuffer := pool.Get()
				encryptedWriter, err := age.Encrypt(encryptedBuffer, recipient.Recipient())
				if err != nil {
					panic(err)
				}

				check(io.Copy(encryptedWriter, compressedChunk.buffer))
				check0(encryptedWriter.Close())
				pool.Put(compressedChunk.buffer)

				compressedAndEncryptedChunks <- IndexedBuffer{
					i:      compressedChunk.i,
					buffer: encryptedBuffer,
				}
			}
		}()
	}

	go func() {
		defer compressedWriter.Close()
		defer compressWriter.Close()
		defer tarWriter.Close()
		defer log.Println("Finished reading files")

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

				log.Println("Writing file to tar", fileName)
				header := &tar.Header{
					Name:    fileName,
					Size:    stat.Size(),
					Mode:    int64(stat.Mode()),
					ModTime: stat.ModTime(),
				}
				check0(tarWriter.WriteHeader(header))
				f := check(os.Open(fileName))
				defer f.Close()

				_, err = io.Copy(tarWriter, f)
				if err != nil {
					panic(err)
				}

				check0(tarWriter.Flush())
				check0(compressWriter.Flush())

				return nil
			}))
		}
	}()

	for task := range compressedAndEncryptedChunks {
		for {
			log.Println("Uploading chunk", task.i)
			_, err := config.MinioClient.PutObject(
				context.Background(),
				config.Config.S3.Bucket,
				fmt.Sprintf("%s/%d", now.Format(time.RFC3339), task.i),
				task.buffer,
				int64(task.buffer.Len()),
				minio.PutObjectOptions{
					ConcurrentStreamParts: true,
					PartSize:              1e7,
					NumThreads:            uint(runtime.NumCPU()),
				},
			)
			log.Println("Uploaded chunk", task.i)

			pool.Put(task.buffer)

			if err == nil {
				pool.Put(task.buffer)
				break
			} else {
				log.Println("MINIO ERROR", err)
				time.Sleep(30 * time.Second)
			}
		}
	}

	log.Println("DONE", time.Since(now))
}
