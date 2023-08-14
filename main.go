package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"

	"filippo.io/age"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	ignore "github.com/sabhiram/go-gitignore"
	"gopkg.in/yaml.v3"
)

func main() {
	doBackup := flag.Bool("backup", false, "Do a full backup")
	doRestore := flag.Bool("restore", false, "Do full restore")
	doSize := flag.Bool("size", false, "Get size of backup on disk")
	doList := flag.Bool("ls", false, "List archives")
	doRemove := flag.Bool("rm", false, "Remove an archive interactively")
	configFileName := flag.String("config", "", "config file locaiton path")

	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("Starting")

	var config BackupConfig

	var name string
	for _, name = range []string{
		*configFileName,
		path.Join(check(os.Getwd()), "backup.yaml"),
		"/etc/backup.yaml",
		path.Join(dismiss(os.UserHomeDir()), "backup.yaml"),
		path.Join(dismiss(os.UserConfigDir()), "backup.yaml"),
	} {
		raw, err := os.ReadFile(name)
		if err == nil {
			check0(yaml.Unmarshal(raw, &config.Config))
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

	if config.Config.Age.Private == "" || config.Config.Age.Public == "" {
		fmt.Printf("!!! We need to generate a private key and saving it to %s, please remember to backup %s to a flashdrive to make restoring easier\n", name, name)

		privateKey, err := age.GenerateX25519Identity()
		if err != nil {
			fmt.Println("Error generating identity:", err)
			return
		}

		config.Config.Age.Private = privateKey.String()

		fmt.Println("Identity Private Key:", privateKey)

		publicKey := privateKey.Recipient()
		config.Config.Age.Public = publicKey.String()

		func() {
			f := check(os.Create(name))
			defer f.Close()
			check0(f.Truncate(0))
			raw := check(yaml.Marshal(config.Config))
			check(f.Write(raw))
			log.Println("Saved keys")
		}()
	}

	if *doList {
		List(&config, false)
	}

	if *doSize {
		Size(&config)
	}

	if *doRemove {
		RemoveArchive(&config)
	}

	if *doBackup {
		Backup(&config)
	}

	if *doRestore {
		Restore(&config)
	}

}
