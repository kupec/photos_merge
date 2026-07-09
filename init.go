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
	chanSize    = 1000
	workerCount = 2
	partSize    = 100
)

var (
	globalMu     sync.Mutex
	handledFiles int
	totalFiles   int
	fileHashes   []fileHash
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("USAGE: %s <DIR>\n", os.Args[0])
		os.Exit(1)
	}

	dirPath := os.Args[1]

	filesChan := traverseFiles(dirPath)
	fileHashesChan := computeHashes(filesChan)
	doneChan := saveHashes(dirPath, fileHashesChan)

	timer := time.NewTimer(0)
	progressPrinter := newProgressPrinter()

	isDone := false
	for !isDone {
		select {
		case <-doneChan:
			isDone = true
		case <-timer.C:
			timer.Reset(time.Second)

			progressPrinter.printProgress()
		}
	}
	progressPrinter.printProgress()
	fmt.Println("")
	fmt.Println("Ready")
}

func traverseFiles(dirPath string) chan string {
	ch := make(chan string, chanSize)

	go func() {
		fileCounter := 0

		walkDir := func(root string) error {
			return filepath.WalkDir(root, func(path string, dir fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if dir.IsDir() {
					return nil
				}

				ch <- path

				fileCounter++
				if fileCounter > 10 {
					globalMu.Lock()
					totalFiles += fileCounter
					fileCounter = 0
					globalMu.Unlock()
				}

				return nil
			})
		}

		if err := walkDir(dirPath); err != nil {
			fmt.Fprintf(os.Stderr, "fail walkDir: %v\n", err)
			os.Exit(1)
		}

		if fileCounter > 0 {
			globalMu.Lock()
			totalFiles += fileCounter
			globalMu.Unlock()
		}

		close(ch)
	}()

	return ch
}

type fileHash struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

func computeHashes(ch chan string) chan fileHash {
	resultChan := make(chan fileHash)

	go func() {
		var wg sync.WaitGroup

		for range workerCount {
			wg.Add(1)

			go func() {
				defer func() {
					wg.Done()
				}()

				for path := range ch {
					hash, err := hashFile(path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "fail hash computating for %s: %v\n", path, err)
						continue
					}

					resultChan <- fileHash{
						Path: path,
						Hash: hash,
					}
				}
			}()
		}

		wg.Wait()
		close(resultChan)
	}()

	return resultChan
}

func hashFile(path string) (string, error) {
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

func saveHashes(dirPath string, fileHashesChan chan fileHash) chan struct{} {
	doneChan := make(chan struct{})

	go func() {
		savePart := func(partIndex int, part []fileHash) {
			jsonBytes, err := json.MarshalIndent(part, "", "  ")
			if err != nil {
				fmt.Fprintf(os.Stderr, "fail to marshal json: %v\n", err)
				os.Exit(1)
			}

			fileName := fmt.Sprintf("index.%d.json", partIndex)
			file, err := os.Create(filepath.Join(dirPath, fileName))
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

			globalMu.Lock()
			handledFiles += len(part)
			globalMu.Unlock()
		}

		partIndex := 0
		part := make([]fileHash, 0, partSize)

		for h := range fileHashesChan {
			part = append(part, h)
			if len(part) == cap(part) {
				savePart(partIndex, part)
				part = part[:0]
				partIndex++
			}
		}

		if len(part) > 0 {
			savePart(partIndex, part)
		}

		doneChan <- struct{}{}
		close(doneChan)
	}()

	return doneChan
}

type progressPrinter struct {
	ticks     int
	tickChars []string
}

func newProgressPrinter() *progressPrinter {
	return &progressPrinter{
		tickChars: []string{"-", `\`, "|", "/"},
	}
}

func (p *progressPrinter) printProgress() {
	tickChar := p.tickChars[p.ticks%len(p.tickChars)]
	p.ticks++

	globalMu.Lock()
	h, t := handledFiles, totalFiles
	globalMu.Unlock()

	progress := 0
	if t > 0 {
		progress = 100 * h / t
	}

	fmt.Printf("\r%s Progress: %3d%% \thandled: %d\ttotal: %d", tickChar, progress, h, t)
}
