package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"filippo.io/age"
	"github.com/minio/minio-go/v7"
)

func getBasenameWithoutExtension(p string) (string, error) {
	filenameWithExt := filepath.Base(p)
	extension := filepath.Ext(filenameWithExt)

	if extension == "" {
		// File has no extension
		return filenameWithExt, nil
	}

	filenameWithoutExt := filenameWithExt[:len(filenameWithExt)-len(extension)]
	return filenameWithoutExt, nil
}

const parallelDownload = 10000

func Restore(config *BackupConfig) {
	now := time.Now()

	downloadedChunk := make(chan IndexedBuffer)

	var downloadWg sync.WaitGroup
	downloadWg.Add(parallelDownload)

	archives := getListOfArchives(config)
	archiveName := archives[len(archives)-1]
	objectChan := getObjectsFromArchives(config, archiveName)

	log.Println("Restoring archive", archiveName)

	go func() {
		for i := 0; i < config.Config.S3.Parallel; i++ {
			go func() {
				defer downloadWg.Done()

				for object := range objectChan {
					if object.Err != nil {
						log.Println("Unable to list object, skipping", object.Err, object)
						continue
					}

					for {
						log.Println("Downloading archive", object.Key)
						file, err := config.MinioClient.GetObject(context.Background(), config.Config.S3.Bucket, object.Key, minio.GetObjectOptions{})
						if err == nil {
							fileBuff := new(bytes.Buffer)
							check(io.Copy(fileBuff, file))
							check0(file.Close())

							index := check(strconv.Atoi(check(getBasenameWithoutExtension(object.Key))))

							downloadedChunk <- IndexedBuffer{fileBuff, index}
							break
						} else {
							time.Sleep(10 * time.Second)
						}
					}
				}
			}()
		}

		downloadWg.Wait()
		close(downloadedChunk)
	}()

	identity := check(age.ParseX25519Identity(config.Config.Age.Private))

	decompressedBufferChan := chanWorker[IndexedBuffer, IndexedBuffer](downloadedChunk, runtime.NumCPU(), func(in IndexedBuffer) IndexedBuffer {
		unencryptedMessage := check(age.Decrypt(in.buffer, identity))
		gzipReader := check(gzip.NewReader(unencryptedMessage))

		unencryptedBuffer := new(bytes.Buffer)
		check(io.Copy(unencryptedBuffer, gzipReader))
		log.Println("Decompressed and decrypted buffer", in.i)

		return IndexedBuffer{
			buffer: unencryptedBuffer,
			i:      in.i,
		}
	})

	rawTarReader := syncronizeBuffers(decompressedBufferChan)
	tarReader := tar.NewReader(rawTarReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			log.Fatal("Error reading archive:", err)
		}

		// Construct the path to restore the file
		targetPath := header.Name

		// Ensure the directory structure exists for the target path
		parentDir := filepath.Dir(targetPath)
		if err := os.MkdirAll(parentDir, os.ModePerm); err != nil {
			log.Fatal("Error creating parent directory:", err)
		}

		// Handle regular file
		file, err := os.Create(targetPath)
		if err != nil {
			log.Fatal("Error creating file:", err)
		}
		defer file.Close()

		// Copy the content from the archive to the file
		_, err = io.Copy(file, tarReader)
		if err != nil {
			log.Fatal("Error copying file content:", err)
		}

		// Set permissions
		if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
			log.Fatal("Error setting file permissions:", err)
		}
	}

	log.Println("DONE", time.Since(now))
}
