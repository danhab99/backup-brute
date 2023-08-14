package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/minio/minio-go/v7"
)

func RemoveArchive(config *BackupConfig) {
	archives := List(config, true)

	fmt.Printf("\n\nChoose which archive you'd like to delete (comma seperated numbers): ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	archivesToDeleteRaw := scanner.Text()

	reader := csv.NewReader(bytes.NewBuffer([]byte(archivesToDeleteRaw)))
	archivesToDeleteRawIndexes := check(reader.Read())

	archivesToDelete := make([]int, len(archivesToDeleteRawIndexes))
	for _, v := range archivesToDeleteRawIndexes {
		archivesToDelete = append(archivesToDelete, check(strconv.Atoi(v)))
	}

	var wg sync.WaitGroup
	wg.Add(len(archivesToDelete))

	for _, v := range archivesToDelete {
		go func(v int) {
			defer wg.Done()
			objects := getObjectsFromArchives(config, archives[v])
			errs := config.MinioClient.RemoveObjects(context.Background(), config.Config.S3.Bucket, objects, minio.RemoveObjectsOptions{})
			for roe := range errs {
				if roe.Err == nil {
				}
			}
		}(v)
	}

	wg.Wait()

}
