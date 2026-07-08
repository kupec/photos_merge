package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	chunkSize   = 10
	chanSize    = 100
	workerCount = 2
)

var (
	globalMu     sync.Mutex
	handledFiles int
	totalFiles   int
	hashMap      map[string]string
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("USAGE: %s <DIR>\n", os.Args[0])
		os.Exit(1)
	}

	dirPath := os.Args[1]

	filesChan := traverseFiles(dirPath)
	hashMapChan := computeHashes(filesChan)

	timer := time.NewTimer(time.Second)
	ticks := 0
	tickChars := []string{"-", `\`, "|", "/"}
	var hashMap map[string]string

	for hashMap == nil {
		select {
		case hashMap = <-hashMapChan:
			break
		case <-timer.C:
			timer.Reset(time.Second)

			ticks++
			tickChar := tickChars[ticks%len(tickChars)]

			globalMu.Lock()
			h, t := handledFiles, totalFiles
			globalMu.Unlock()

			progress := 100 * h / t
			fmt.Printf("\r%s Progress: %d percent\thandled: %d\ttotal: %d", tickChar, progress, h, t)
		}
	}

	fmt.Println("")
	fmt.Println("")
	fmt.Println("Result:")

	jsonBytes, err := json.MarshalIndent(hashMap, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to marshal json: %v\n", err)
		os.Exit(1)
	}

	file, err := os.Create(filepath.Join(dirPath, "index.json"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to create json file: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = file.Close()
	}()

	if _, err := file.Write(jsonBytes); err != nil {
		fmt.Fprintf(os.Stderr, "fail to write json file: %v\n", err)
		os.Exit(1)
	}
}

func traverseFiles(dirPath string) chan []string {
	ch := make(chan []string, chanSize)

	go func() {
		chunk := make([]string, 0, chunkSize)

		walkDir := func(root string) error {
			return filepath.WalkDir(root, func(path string, dir fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if dir.IsDir() {
					return nil
				}

				chunk = append(chunk, path)
				if len(chunk) == chunkSize {
					ch <- chunk

					globalMu.Lock()
					totalFiles += len(chunk)
					globalMu.Unlock()

					chunk = make([]string, 0, chunkSize)
				}

				return nil
			})
		}

		if err := walkDir(dirPath); err != nil {
			fmt.Fprintf(os.Stderr, "fail walkDir: %v\n", err)
			os.Exit(1)
		}

		close(ch)
	}()

	return ch
}

func computeHashes(ch chan []string) chan map[string]string {
	resultChan := make(chan map[string]string)

	go func() {
		var (
			mu sync.Mutex
			wg sync.WaitGroup
		)

		result := make(map[string]string)

		for range workerCount {
			wg.Add(1)

			go func() {
				defer func() {
					wg.Done()
				}()

				for pathChunk := range ch {
					for _, path := range pathChunk {
						hash, err := fileHash(path)
						if err != nil {
							fmt.Fprintf(os.Stderr, "fail hash computating for %s: %v\n", path, err)
							continue
						}

						mu.Lock()
						result[hash] = path
						mu.Unlock()
					}

					globalMu.Lock()
					handledFiles += len(pathChunk)
					globalMu.Unlock()
				}
			}()
		}

		wg.Wait()
		resultChan <- result
		close(resultChan)
	}()

	return resultChan
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("fail open path=%s: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("fail sha256: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
