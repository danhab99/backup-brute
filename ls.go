package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7"
)

func getListOfArchives(config *BackupConfig) []time.Time {
	archivesChan := config.MinioClient.ListObjects(context.Background(), config.Config.S3.Bucket, minio.ListObjectsOptions{
		// Recursive: true,
	})

	var archives []time.Time

	for archive := range archivesChan {
		archives = append(archives, check(time.Parse(time.RFC3339, archive.Key[:strings.Index(archive.Key, "/")])))
	}

	sort.SliceStable(archives, func(i, j int) bool {
		return archives[i].Before(archives[j])
	})

	return archives
}

func getObjectsFromArchives(config *BackupConfig, archive time.Time) <-chan minio.ObjectInfo {
	archiveName := archive.Format(time.RFC3339)
	return config.MinioClient.ListObjects(context.Background(), config.Config.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    archiveName,
		Recursive: true,
	})
}

type ArchiveMap map[time.Time]uint64

func List(config *BackupConfig) {
	archives := getListOfArchives(config)

	cachedArchives := make(ArchiveMap)
	var cachedArchivesLock sync.Mutex
	archiveCache, err := os.ReadFile("/var/cache/backup-brute/archivesizes.json")
	if err == nil {
		json.Unmarshal(archiveCache, &cachedArchives)
	}

	cachedKeys := make([]time.Time, 0, len(cachedArchives))
	for k := range cachedArchives {
		cachedKeys = append(cachedKeys, k)
	}

	archivesToDownload, _, archivesToDelete := vennDiff[time.Time](archives, cachedKeys)

	for _, archive := range archivesToDelete {
		delete(cachedArchives, archive)
	}

	var wg sync.WaitGroup
	wg.Add(len(archives))

	for _, archive := range archivesToDownload {
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

			cachedArchivesLock.Lock()
			cachedArchives[archive] = size
			cachedArchivesLock.Unlock()
		}(archive)
	}

	wg.Wait()

	// maxLen := math.MaxInt
	maxLen := 0
	for t, _ := range cachedArchives {
		k := t.String()
		if len(k) > maxLen {
			maxLen = len(k)
		}
	}

	for k, v := range cachedArchives {
		fmt.Printf("%-*s %s\n", maxLen, k, humanize.Bytes(uint64(v)))
	}
}
