package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7"
)

type ArchiveMap map[time.Time]uint64

func List(config *BackupConfig, showNums bool) (out []time.Time) {
	archives := getListOfArchives(config)
	archiveMap := make(ArchiveMap)
	var archiveMapLock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(archives))

	for _, archive := range archives {
		archiveName := archive.Format(time.RFC3339)
		go func(archive time.Time) {
			defer wg.Done()

			objectChan := config.MinioClient.ListObjects(context.Background(), config.Config.S3.Bucket, minio.ListObjectsOptions{
				Prefix:       archiveName,
				WithMetadata: true,
				Recursive:    true,
			})

			size := uint64(0)

			for object := range objectChan {
				if object.Err == nil {
					size += uint64(object.Size)
				}
			}

			archiveMapLock.Lock()
			archiveMap[archive] = size
			archiveMapLock.Unlock()
		}(archive)
	}

	wg.Wait()

	// maxLen := math.MaxInt
	maxLen := 0
	for t, _ := range archiveMap {
		k := t.String()
		if len(k) > maxLen {
			maxLen = len(k)
		}
	}

	for k, v := range archiveMap {
		if showNums {
			fmt.Printf("  %d) ", len(out))
		}
		out = append(out, k)
		fmt.Printf("%-*s %s\n", maxLen, k, humanize.Bytes(uint64(v)))
	}

	return
}
