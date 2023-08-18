package main

import (
	"bytes"
	"os"

	"github.com/minio/minio-go/v7"
	ignore "github.com/sabhiram/go-gitignore"
	"golang.org/x/crypto/openpgp"
)

type IndexedBuffer struct {
	buffer *bytes.Buffer
	i      int
}

type NamedBuffer struct {
	filepath string
	info     os.FileInfo
	buffer   *bytes.Buffer
}

type Config struct {
	S3 struct {
		Access   string `yaml:"access"`
		Secret   string `yaml:"secret"`
		Region   string `yaml:"region"`
		Endpoint string `yaml:"endpoint"`
		Bucket   string `yaml:"bucket"`
		// Parallel int    `yaml:"parallel"`
	} `yaml:"s3"`

	Age struct {
		Private string `yaml:"private"`
		Public  string `yaml:"public"`
	} `yaml:"age"`

	Memory struct {
		ChunkCount string `yaml:"chunkCount"`
		ChunkSize  string `yaml:"chunkSize"`
		Upload     string `yaml:"upload"`
	} `yaml:"memory"`

	IncludeDirs     []string `yaml:"includeDirs"`
	ExcludePatterns []string `yaml:"excludePatterns"`

	DryRun bool `yaml:"dryrun"`
}

type BackupConfig struct {
	MinioClient *minio.Client
	Ignorer     ignore.IgnoreParser
	Entities    openpgp.EntityList

	Config Config

	chunkSize  uint64
	chunkCount int
	upload     uint64
}

type ReaderWithLength interface {
	Read(p []byte) (n int, err error)
	Len() int
}
