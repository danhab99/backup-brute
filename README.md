# Backup Brute

Go utility for creating backups of large segments of your operating system to s3 using [AGE](https://github.com/FiloSottile/age) encryption.

## Install

Arch/Manjaro: `yay -S backup-brute-git`

Go module: `go install github.com/danhab99/backup-brute@1.0`

## Usage

```
Usage of backup-brute:
  -backup
    	Do a full backup
  -config string
    	config file locaiton path
  -ls
    	List archives
  -restore
    	Do full restore
  -size
    	Get size of backup on disk
```

## `backup.yaml`

Backup Brute will generate encryption keys and store them in your `backup.yaml`. Your `backup.yaml` contains all the information you need to restore your system, please make sure to keep it a secret.

`backup.yaml` locaiton priority:

- `--config` file arg
- `$(PWD)/backup.yaml`
- `/etc/backup.yaml`
- `$HOME/backup.yaml`
- `$HOME/.config/backup.yaml`

```yaml
s3:
    access: [SET YOUR S3 ACCESS KEY HERE]
    secret: [SET YOUR S3 SECRET KEY HERE]
    region: us-east-1
    endpoint: sjc1.vultrobjects.com
    bucket: laptop
age:
    private: [GENERATED WHEN YOU DO YOUR FIRST BACKUP]
    public: [GENERATED WHEN YOU DO YOUR FIRST BACKUP]

chunkSize: 10000000 # Size in bytes of the chunks that get uploaded to s3

includeDirs:
    - /home
    - /etc
    - /usr
    - /srv
    - /opt

excludePatterns: # Takes gitignore styled glob patterns
    - node_modules
    - '*cache*'
    - '*Cache*'
    - .local
    - .var
    - .npm
    - .go/src
    - .go/pkg'
    - .wine
    - .next
    - .nuget
    - /etc/pacman.d/gnupg/
    - /etc/zsh
    - /etc/subgid
    - /etc/subuid
    - /etc/xdg/pacaur/config
    - tmp
    - temp
    - Temp
```
