package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"reflect"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	ignore "github.com/sabhiram/go-gitignore"
	progressbar "github.com/schollz/progressbar/v3"
	"golang.org/x/crypto/openpgp"
	"gopkg.in/yaml.v3"
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
	} `yaml:"gpg"`

	ChunkSize       int64    `yaml:"chunkSize"`
	IncludeDirs     []string `yaml:"includeDirs"`
	ExcludePatterns []string `yaml:"excludePatterns"`

	DryRun bool `yaml:"dryrun"`

	minioClient *minio.Client
	ignorer     ignore.IgnoreParser
	entities    openpgp.EntityList
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

type ReaderWithLength interface {
	Read(p []byte) (n int, err error)
	Len() int
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

func main() {
	doBackup := flag.Bool("backup", false, "Do a full backup")
	// doRestore := flag.Bool("restore", false, "Do full restore")
	doSize := flag.Bool("size", false, "Get size of backup on disk")

	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("Starting")

	var config Config

	for _, filepath := range []string{
		"/etc/backup.yaml",
		path.Join(check(os.UserHomeDir()), "backup.yaml"),
		path.Join(check(os.UserConfigDir()), "backup.yaml"),
		path.Join(check(os.Getwd()), "backup.yaml"),
	} {
		raw, err := os.ReadFile(filepath)
		if err != nil {
			continue
		}
		check0(yaml.Unmarshal(raw, &config))
		if AllFieldsDefined(config) {
			break
		}
	}

	log.Printf("Config: %+v\n ", config)

	config.ignorer = ignore.CompileIgnoreLines(config.ExcludePatterns...)

	creds := credentials.NewStaticV4(config.S3.Access, config.S3.Secret, "")

	config.minioClient = check(minio.New(config.S3.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: true,
		Region: config.S3.Region,
	}))

	privateKey := check(os.Open(config.GPG.PrivateKeyFile))
	defer privateKey.Close()

	config.entities = check(openpgp.ReadArmoredKeyRing(privateKey))

	if *doSize {
		Size(config)
	}

	if *doBackup {
		Backup(config)
	}

}
