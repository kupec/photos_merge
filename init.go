package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	chanSize    = 1000
	workerCount = 4
	partSize    = 100
)

var (
	globalMu       sync.Mutex
	handledFiles   int
	totalFiles     int
	fileHashes     []fileHash
	indexFileRegex = regexp.MustCompile(`index.(\d+).json`)
	tr             trace.Tracer
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

	absDirPath, err := filepath.Abs(dirPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to get absolute path: %v\n", err)
		os.Exit(1)
	}
	dirPath = absDirPath

	tp := newTraceProvider()
	defer tp.Shutdown(context.Background())

	tr = tp.Tracer("init tool")

	ctx, span := tr.Start(context.Background(), "main")
	defer span.End()

	_, spanLoadPreviousRun := tr.Start(ctx, "loadPreviousRun")

	previousRun, err := loadPreviousRun(dirPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail load previous run: %v\n", err)
		os.Exit(1)
	}

	spanLoadPreviousRun.End()
	_, spanRestMain := tr.Start(ctx, "rest-main")
	defer spanRestMain.End()

	handledFiles = len(previousRun.fileHashMap)
	totalFiles = handledFiles

	filesChan := traverseFiles(ctx, dirPath, previousRun)
	fileHashesChan := computeHashes(ctx, filesChan)
	doneChan := saveHashes(ctx, dirPath, fileHashesChan, previousRun)

	timer := time.NewTimer(0)
	progressPrinter := newProgressPrinter()

	isDone := false
	for !isDone {
		select {
		case <-doneChan:
			isDone = true
		case <-timer.C:
			timer.Reset(100 * time.Millisecond)

			progressPrinter.printProgress()
		}
	}
	progressPrinter.printProgress()
	fmt.Println("")
	fmt.Println("Ready")
}

func newTraceProvider() *sdktrace.TracerProvider {
	if os.Getenv("DEBUG") != "1" {
		return sdktrace.NewTracerProvider()
	}

	endpoint := "192.168.122.78:4318"
	exporter, err := otlptracehttp.New(
		context.Background(),
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	rsc, err := resource.New(context.Background(), resource.WithAttributes(
		semconv.ServiceNameKey.String("init tool"),
		semconv.TelemetrySDKLanguageKey.String("go"),
	))
	if err != nil {
		panic(err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(rsc),
	)
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

func traverseFiles(ctx context.Context, dirPath string, previousRun *PreviousRun) chan string {
	ch := make(chan string, chanSize)

	go func() {
		ctx, span := tr.Start(ctx, "traverseFiles")
		defer span.End()

		fileCounter := 0

		walkDir := func(root string) error {
			return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}

				if indexFileRegex.MatchString(entry.Name()) || entry.Name() == "index.json" {
					return nil
				}

				relPath, err := filepath.Rel(dirPath, path)
				if err != nil {
					return fmt.Errorf("fail to get relative path for %s: %w", path, err)
				}

				if _, ok := previousRun.fileHashMap[relPath]; ok {
					return nil
				}

				_, chSpan := tr.Start(ctx, "traverseFiles: wait for channel to write")

				ch <- path

				chSpan.End()

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

func computeHashes(ctx context.Context, ch chan string) chan fileHash {
	resultChan := make(chan fileHash, chanSize)

	go func() {
		var wg sync.WaitGroup

		for range workerCount {
			wg.Add(1)

			go func() {
				defer wg.Done()

				ctx, span := tr.Start(ctx, "computeHashes:worker")
				defer span.End()

				_, span = tr.Start(ctx, "computeHashes: wait for reading channel")

				for path := range ch {
					span.End()
					_, span = tr.Start(ctx, "hashFile")

					hash, err := hashFile(path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "fail hash computating for %s: %v\n", path, err)
						continue
					}

					span.End()
					_, span = tr.Start(ctx, "computeHashes: wait for writing to channel")

					resultChan <- fileHash{
						Path: path,
						Hash: hash,
					}

					span.End()
					_, span = tr.Start(ctx, "computeHashes: wait for reading channel")
				}

				span.End()
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

func saveHashes(ctx context.Context, dirPath string, fileHashesChan chan fileHash, previousRun *PreviousRun) chan struct{} {
	doneChan := make(chan struct{})

	go func() {
		ctx, span := tr.Start(ctx, "saveHashes")
		defer span.End()

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

		fileHashMap := maps.Clone(previousRun.fileHashMap)

		partIndex := previousRun.nextPartIndex
		part := make([]fileHash, 0, partSize)

		_, span = tr.Start(ctx, "saveHashes: wait for reading channel")

		for h := range fileHashesChan {
			span.End()

			relPath, err := filepath.Rel(dirPath, h.Path)
			if err != nil {
				relPath = h.Path
			}

			fileHashMap[relPath] = h.Hash

			part = append(part, fileHash{
				Path: relPath,
				Hash: h.Hash,
			})
			if len(part) == cap(part) {
				_, span = tr.Start(ctx, "savePart")

				savePart(partIndex, part)
				part = part[:0]
				partIndex++

				span.End()
			}

			_, span = tr.Start(ctx, "saveHashes: wait for reading channel")
		}

		span.End()

		if len(part) > 0 {
			_, span = tr.Start(ctx, "savePart")

			savePart(partIndex, part)

			span.End()
		}

		fileHashes := make([]fileHash, 0, len(fileHashMap))
		for path, hash := range fileHashMap {
			fileHashes = append(fileHashes, fileHash{
				Path: path,
				Hash: hash,
			})
		}

		_, span = tr.Start(ctx, "saveHashesToFile")

		err := saveHashesToFile(filepath.Join(dirPath, "index.json"), fileHashes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fail to save final hashes: %v\n", err)
			os.Exit(1)
		}

		span.End()
		_, span = tr.Start(ctx, "removePreviousRun")

		if err := removePreviousRun(dirPath); err != nil {
			fmt.Fprintf(os.Stderr, "fail to remove previous run: %v\n", err)
			os.Exit(1)
		}

		span.End()

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
	defer file.Close()

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
		tickChars: []string{"⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"},
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
