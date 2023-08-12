package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
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

	var wg sync.WaitGroup
	wg.Add(3)

	encryptedBuffChan := make(chan IndexedBuffer)

	go func() {
		defer wg.Done()

		var downloadWg sync.WaitGroup
		downloadWg.Add(parallelDownload)

		archivesChan := config.MinioClient.ListObjects(context.Background(), config.Config.S3.Bucket, minio.ListObjectsOptions{
			Recursive: true,
		})

		var archives []time.Time

		for archive := range archivesChan {
			archives = append(archives, check(time.Parse(time.RFC3339, archive.Key[:strings.Index(archive.Key, "/")])))
		}

		sort.SliceStable(archives, func(i, j int) bool {
			return archives[i].Before(archives[j])
		})

		archiveName := archives[len(archives)-1].Format(time.RFC3339)
		objectChan := config.MinioClient.ListObjects(context.Background(), config.Config.S3.Bucket, minio.ListObjectsOptions{
			Prefix:    archiveName,
			Recursive: true,
		})

		for i := 0; i < parallelDownload; i++ {
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

							encryptedBuffChan <- IndexedBuffer{fileBuff, index}
							break
						} else {
							time.Sleep(10 * time.Second)
						}
					}
				}
			}()
		}

		downloadWg.Wait()
		close(encryptedBuffChan)
	}()

	tarPipeReader, tarWriter := io.Pipe()
	tarReader := tar.NewReader(tarPipeReader)

	go func() {
		defer wg.Done()

		var decryptWg sync.WaitGroup
		decryptWg.Add(runtime.NumCPU())

		buffMap := make(map[int]*bytes.Buffer)
		var buffLock sync.Mutex

		count := 0

		identity := check(age.ParseX25519Identity(config.Config.Age.Private))

		for i := 0; i < runtime.NumCPU(); i++ {
			go func() {
				defer decryptWg.Done()

				for encryptedBuff := range encryptedBuffChan {
					unencryptedMessage := check(age.Decrypt(encryptedBuff.buffer, identity))

					buffLock.Lock()
					buffMap[encryptedBuff.i] = new(bytes.Buffer)
					check(io.Copy(buffMap[encryptedBuff.i], unencryptedMessage))

					for buf, ok := buffMap[count]; ok; {
						io.Copy(tarWriter, buf)
						delete(buffMap, count)
						count++
					}

					buffLock.Unlock()
				}
			}()

		}
	}()

	go func() {
		defer wg.Done()

		for {
			header := check(tarReader.Next())
			log.Println("Writing file", header)
			f := check(os.OpenFile(header.Name, int(header.Mode), fs.FileMode(header.Mode)))
			n := check(io.Copy(f, tarReader))
			if n != header.Size {
				panic(fmt.Sprintf("Failed to save file properly %s", header.Name))
			}
		}

	}()

	wg.Wait()
	log.Println("DONE", time.Since(now))
}
