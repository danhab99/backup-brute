package main

import (
	"archive/tar"
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
	"time"

	"filippo.io/age"
	"github.com/minio/minio-go/v7"
)

func Backup(config *BackupConfig) {
	now := time.Now()
	pool := NewBufferPool()
	recipient := check(age.ParseX25519Identity(config.Config.Age.Private))

	tarReader, tarPipeWriter := io.Pipe()
	tarWriter := tar.NewWriter(tarPipeWriter)

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

				log.Println("Writing file to tar", fileName)
				header := &tar.Header{
					Name:    fileName,
					Size:    stat.Size(),
					Mode:    int64(stat.Mode()),
					ModTime: stat.ModTime(),
				}
				check0(tarWriter.WriteHeader(header))
				f := check(os.Open(fileName))
				io.Copy(tarWriter, f)
				check0(f.Close())

				return nil
			}))
		}
	}()

	tarChunkChan := makeChunks(tarReader, &pool, int64(config.chunkSize))

	encryptedBufferChan := chanWorker[IndexedBuffer, IndexedBuffer](tarChunkChan, runtime.NumCPU(), func(in IndexedBuffer) IndexedBuffer {
		log.Println("Compressing chunk", in.i)
		compressedBuffer := pool.Get()

		gzipWriter := check(gzip.NewWriterLevel(compressedBuffer, gzip.BestCompression))
		encryptWriter := check(age.Encrypt(gzipWriter, recipient.Recipient()))

		check(io.Copy(encryptWriter, in.buffer))
		pool.Put(in.buffer)

		check0(encryptWriter.Close())
		check0(gzipWriter.Flush())
		check0(gzipWriter.Close())

		return IndexedBuffer{
			buffer: compressedBuffer,
			i:      in.i,
		}
	})

	waitChan := chanWorker[IndexedBuffer, any](encryptedBufferChan, config.Config.S3.Parallel, func(task IndexedBuffer) any {
		for {
			log.Println("Uploading chunk", task.i)
			_, err := config.MinioClient.PutObject(
				context.Background(),
				config.Config.S3.Bucket,
				fmt.Sprintf("%s/%d", now.Format(time.RFC3339), task.i),
				task.buffer,
				int64(task.buffer.Len()),
				minio.PutObjectOptions{
					// ConcurrentStreamParts: true,
					// NumThreads:            uint(runtime.NumCPU()),
				},
			)
			log.Println("Uploaded chunk", task.i)

			pool.Put(task.buffer)

			if err == nil {
				break
			} else {
				log.Println("MINIO ERROR", err)
				time.Sleep(30 * time.Second)
			}
		}

		return nil
	})

	for e := range waitChan {
		if e == nil {
		}
	}

	log.Println("DONE", time.Since(now))
}
