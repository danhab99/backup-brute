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
	fileNameChan := make(chan NamedBuffer)
	pool := NewBufferPool()
	recipient := check(age.ParseX25519Identity(config.Config.Age.Private))

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
		log.Println("Reading file", in.filepath)

		buff := pool.Get()
		f := check(os.Open(in.filepath))
		check(io.Copy(buff, f))
		check0(f.Close())

		return NamedBuffer{
			filepath: in.filepath,
			info:     in.info,
			buffer:   buff,
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
			log.Println("Writing to tar file", namedbuffer.info.Name())
			header := &tar.Header{
				Name:    namedbuffer.filepath,
				Size:    int64(namedbuffer.buffer.Len()),
				Mode:    int64(namedbuffer.info.Mode()),
				ModTime: namedbuffer.info.ModTime(),
			}
			check0(tarWriter.WriteHeader(header))
			check(io.Copy(tarWriter, namedbuffer.buffer))
			pool.Put(namedbuffer.buffer)
		}
	}()

	tarChunkChan := makeChunks(tarReader, &pool, int64(config.chunkSize))

	encryptedBufferChan := chanWorker[IndexedBuffer, IndexedBuffer](tarChunkChan, runtime.NumCPU(), func(in IndexedBuffer) IndexedBuffer {
		log.Println("Compressing chunk", in.i)
		compressedBuffer := pool.Get()

		gzipWriter := check(gzip.NewWriterLevel(compressedBuffer, gzip.BestCompression))
		encryptWriter := check(age.Encrypt(gzipWriter, recipient.Recipient()))

		check(io.Copy(encryptWriter, in.buffer))

		check0(encryptWriter.Close())
		check0(gzipWriter.Flush())
		check0(gzipWriter.Close())
		pool.Put(in.buffer)

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
					ConcurrentStreamParts: true,
					NumThreads:            uint(runtime.NumCPU()),
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
