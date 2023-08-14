package main

import (
	"context"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	progressbar "github.com/schollz/progressbar/v3"
)

func check[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func check0(err error) {
	if err != nil {
		panic(err)
	}
}

func dismiss[T any](v T, err error) T {
	return v
}

func AllFieldsDefined(v interface{}) bool {
	value := reflect.ValueOf(v)
	if value.Kind() != reflect.Struct {
		return false
	}

	numFields := value.NumField()
	for i := 0; i < numFields; i++ {
		fieldValue := value.Field(i)
		if reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldValue.Type()).Interface()) {
			return false
		}
	}

	return true
}

func copyProgress(writer io.Writer, reader ReaderWithLength, label string) (int64, error) {
	bar := progressbar.DefaultBytes(
		int64(reader.Len()),
		label,
	)
	defer bar.Close()
	return io.Copy(io.MultiWriter(writer, bar), reader)
}

func copyProgressN(writer io.Writer, reader io.Reader, n int64, label string) (int64, error) {
	bar := progressbar.DefaultBytes(n, label)
	defer bar.Close()
	return io.CopyN(io.MultiWriter(writer, bar), reader, n)
}
func contains[T comparable](s []T, e T) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func vennDiff[T comparable](left, right []T) (keysInLeft, keysInBoth, keysInRight []T) {
	for _, leftK := range left {
		inBoth := contains[T](right, leftK)
		if inBoth {
			keysInBoth = append(keysInBoth, leftK)
		} else {
			keysInLeft = append(keysInLeft, leftK)
		}
	}

	for _, rightK := range right {
		inLeft := contains[T](left, rightK)
		if !inLeft {
			keysInRight = append(keysInRight, rightK)
		}
	}

	return
}

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
