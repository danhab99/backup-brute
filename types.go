package main

import (
	"bytes"

	"github.com/minio/minio-go/v7"
	ignore "github.com/sabhiram/go-gitignore"
	"golang.org/x/crypto/openpgp"
)

type IndexedBuffer struct {
	buffer *bytes.Buffer
	i      int
}

type Config struct {
	S3 struct {
		Access   string `yaml:"access"`
		Secret   string `yaml:"secret"`
		Region   string `yaml:"region"`
		Endpoint string `yaml:"endpoint"`
		Bucket   string `yaml:"bucket"`
	} `yaml:"s3"`

	GPG struct {
		PrivateKeyFile string `yaml:"privateKeyFile"`
		PublicKeyFile  string `yaml:"publicKeyFile"`
	} `yaml:"gpg"`

	ChunkSize       int64    `yaml:"chunkSize"`
	IncludeDirs     []string `yaml:"includeDirs"`
	ExcludePatterns []string `yaml:"excludePatterns"`

	DryRun bool `yaml:"dryrun"`
}

type BackupConfig struct {
	MinioClient *minio.Client
	Ignorer     ignore.IgnoreParser
	Entities    openpgp.EntityList

	Config Config
}

type ReaderWithLength interface {
	Read(p []byte) (n int, err error)
	Len() int
}
