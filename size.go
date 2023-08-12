package main

import (
	"io/fs"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
)

func Size(config BackupConfig) {

	size := int64(0)
	var wg sync.WaitGroup
	wg.Add(len(config.Config.IncludeDirs))

	for _, dir := range config.Config.IncludeDirs {
		go func(dir string) {
			defer wg.Done()
			check0(filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
				if err != nil {
					return nil
				}

				if info.IsDir() {
					return nil
				}

				if config.Ignorer.MatchesPath(strings.ToLower(path)) {
					return nil
				}

				size += info.Size()

				return nil
			}))
		}(dir)
	}

	wg.Wait()

	log.Println("Total backup size", humanize.Bytes(uint64(size)))
}
