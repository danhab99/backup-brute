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
	"time"

	"filippo.io/age"
	"github.com/minio/minio-go/v7"
	progressbar "github.com/schollz/progressbar/v3"
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

	fileBufferChan := chanWorker[NamedBuffer, NamedBuffer](fileNameChan, 10, func(in NamedBuffer) NamedBuffer {
		log.Println("Reading file", in.filepath)
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
			log.Println("Writing to tar file", namedbuffer.info.Name())
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

	tarChunkChan := makeChunks(tarReader, config.Config.ChunkSize)

	compressedBuffersChan := chanWorker[IndexedBuffer, IndexedBuffer](tarChunkChan, runtime.NumCPU(), func(in IndexedBuffer) IndexedBuffer {
		log.Println("Compressing chunk", in.i)
		compressedBuffer := new(bytes.Buffer)
		gzipWriter := gzip.NewWriter(compressedBuffer)
		check(io.Copy(gzipWriter, in.buffer))
		check0(gzipWriter.Close())

		return IndexedBuffer{
			buffer: compressedBuffer,
			i:      in.i,
		}
	})

	encryptedBufferChan := chanWorker[IndexedBuffer, IndexedBuffer](compressedBuffersChan, runtime.NumCPU(), func(task IndexedBuffer) IndexedBuffer {
		encryptedBuffer := bytes.NewBuffer(make([]byte, 0, task.buffer.Len()))
		log.Printf("Encrypted chunk %d", task.i)

		recipient := check(age.ParseX25519Identity(config.Config.Age.Private))
		encryptWriter := check(age.Encrypt(encryptedBuffer, recipient.Recipient()))
		check(io.Copy(encryptWriter, task.buffer))
		check0(encryptWriter.Close())

		return IndexedBuffer{
			buffer: encryptedBuffer,
			i:      task.i,
		}
	})

	waitChan := chanWorker[IndexedBuffer, any](encryptedBufferChan, 100, func(task IndexedBuffer) any {
		for {
			bar := progressbar.DefaultBytes(int64(task.buffer.Len()), fmt.Sprintf("Uploading %d", task.i))

			_, err := config.MinioClient.PutObject(
				context.Background(),
				config.Config.S3.Bucket,
				fmt.Sprintf("%s/%d", now.Format(time.RFC3339), task.i),
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

		return nil
	})

	for e := range waitChan {
		if e == nil {
		}
	}

	log.Println("DONE", time.Since(now))
}
