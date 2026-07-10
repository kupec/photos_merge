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
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	chanSize    = 1000
	workerCount = 2
	partSize    = 100
)

var (
	globalMu       sync.Mutex
	handledFiles   int
	totalFiles     int
	fileHashes     []fileHash
	indexFileRegex = regexp.MustCompile(`index.(\d+).json`)
)

type PreviousRun struct {
	fileHashMap   map[string]string
	nextPartIndex int
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "USAGE: %s <DIR>\n", os.Args[0])
		os.Exit(1)
	}

	dirPath := os.Args[1]

	previousRun, err := loadPreviousRun(dirPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail load previous run: %v\n", err)
		os.Exit(1)
	}

	filesChan := traverseFiles(dirPath)
	fileHashesChan := computeHashes(filesChan, previousRun)
	doneChan := saveHashes(dirPath, fileHashesChan, previousRun)

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

func loadPreviousRun(dirPath string) (*PreviousRun, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("fail read dir %s: %w", dirPath, err)
	}

	nextPartIndex := 0
	fileHashMap := make(map[string]string)

	for _, entry := range entries {
		matches := indexFileRegex.FindStringSubmatch(entry.Name())
		if len(matches) == 0 {
			continue
		}

		partIndex, err := strconv.Atoi(matches[1])
		if err != nil {
			continue
		}

		if partIndex >= nextPartIndex {
			nextPartIndex = partIndex + 1
		}

		entryPath := filepath.Join(dirPath, entry.Name())
		file, err := os.Open(entryPath)
		if err != nil {
			return nil, fmt.Errorf("fail open %s: %w", entryPath, err)
		}

		var hashes []fileHash
		if err := json.NewDecoder(file).Decode(&hashes); err != nil {
			return nil, fmt.Errorf("fail decode %s: %w", entryPath, err)
		}

		for _, hash := range hashes {
			fileHashMap[hash.Path] = hash.Hash
		}
	}

	return &PreviousRun{
		fileHashMap:   fileHashMap,
		nextPartIndex: nextPartIndex,
	}, nil
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

func computeHashes(ch chan string, previousRun *PreviousRun) chan fileHash {
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
					if _, ok := previousRun.fileHashMap[path]; ok {
						continue
					}

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

func saveHashes(dirPath string, fileHashesChan chan fileHash, previousRun *PreviousRun) chan struct{} {
	doneChan := make(chan struct{})

	go func() {
		savePart := func(partIndex int, part []fileHash) {
			fileName := fmt.Sprintf("index.%d.json", partIndex)
			err := saveHashesToFile(filepath.Join(dirPath, fileName), part)
			if err != nil {
				fmt.Fprintf(os.Stderr, "fail to save hashes part: %v\n", err)
				os.Exit(1)
			}

			globalMu.Lock()
			handledFiles += len(part)
			globalMu.Unlock()
		}

		fileHashMap := previousRun.fileHashMap

		partIndex := previousRun.nextPartIndex
		part := make([]fileHash, 0, partSize)

		for h := range fileHashesChan {
			fileHashMap[h.Path] = h.Hash

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

		fileHashes := make([]fileHash, 0, len(fileHashMap))
		for path, hash := range fileHashMap {
			fileHashes = append(fileHashes, fileHash{
				Path: path,
				Hash: hash,
			})
		}

		err := saveHashesToFile(filepath.Join(dirPath, "index.json"), fileHashes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fail to save final hashes: %v\n", err)
			os.Exit(1)
		}

		if err := removePreviousRun(dirPath); err != nil {
			fmt.Fprintf(os.Stderr, "fail to remove previous run: %v\n", err)
			os.Exit(1)
		}

		doneChan <- struct{}{}
		close(doneChan)
	}()

	return doneChan
}

func saveHashesToFile(filePath string, part []fileHash) error {
	jsonBytes, err := json.MarshalIndent(part, "", "  ")
	if err != nil {
		return fmt.Errorf("fail to marshal json: %w\n", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("fail to create json file: %w\n", err)
	}
	defer func() {
		_ = file.Close()
	}()

	if _, err := file.Write(jsonBytes); err != nil {
		return fmt.Errorf("fail to write json file: %w\n", err)
	}

	return nil
}

func removePreviousRun(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("fail read dir %s: %w", dirPath, err)
	}

	for _, entry := range entries {
		if !indexFileRegex.MatchString(entry.Name()) {
			continue
		}

		entryPath := filepath.Join(dirPath, entry.Name())
		if err := os.Remove(entryPath); err != nil {
			return fmt.Errorf("fail remove %s: %w", entryPath, err)
		}
	}

	return nil
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
