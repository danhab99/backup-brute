package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	ignore "github.com/sabhiram/go-gitignore"
	"golang.org/x/crypto/openpgp"
	"gopkg.in/yaml.v3"
)

func main() {
	doBackup := flag.Bool("backup", false, "Do a full backup")
	doRestore := flag.Bool("restore", false, "Do full restore")
	doSize := flag.Bool("size", false, "Get size of backup on disk")

	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("Starting")

	var config BackupConfig

	fmt.Println(os.Getwd())

	for _, filepath := range []string{
		path.Join(check(os.Getwd()), "backup.yaml"),
		"/etc/backup.yaml",
		path.Join(check(os.UserHomeDir()), "backup.yaml"),
		path.Join(check(os.UserConfigDir()), "backup.yaml"),
	} {
		raw, err := os.ReadFile(filepath)
		if err != nil {
			continue
		}
		check0(yaml.Unmarshal(raw, &config.Config))
		if AllFieldsDefined(config.Config) {
			break
		}
	}

	log.Printf("Config: %+v\n ", config)

	config.Ignorer = ignore.CompileIgnoreLines(config.Config.ExcludePatterns...)

	creds := credentials.NewStaticV4(config.Config.S3.Access, config.Config.S3.Secret, "")

	config.MinioClient = check(minio.New(config.Config.S3.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: true,
		Region: config.Config.S3.Region,
	}))

	privateKey := check(os.Open(config.Config.GPG.PrivateKeyFile))
	defer privateKey.Close()

	config.Entities = check(openpgp.ReadArmoredKeyRing(privateKey))

	if *doSize {
		Size(config)
	}

	if *doBackup {
		Backup(config)
	}

	if *doRestore {
		Restore(config)
	}
}
