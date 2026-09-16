package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

var tr trace.Tracer

type fileHash struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

type walEntry struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "USAGE: %s <FROM_DIR> <TO_DIR>\n", os.Args[0])
		os.Exit(1)
	}

	fromDir := os.Args[1]
	toDir := os.Args[2]

	tp := newTraceProvider()
	defer tp.Shutdown(context.Background())
	tr = tp.Tracer("merge tool")

	ctx, span := tr.Start(context.Background(), "main")
	defer span.End()

	for _, dir := range []string{fromDir, toDir} {
		if !fileExists(filepath.Join(dir, "index.json")) {
			fmt.Fprintf(os.Stderr, "index.json not found in %s\n", dir)
			os.Exit(1)
		}
	}

	toIndex, toHashSet, err := loadIndex(filepath.Join(toDir, "index.json"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail load to index: %v\n", err)
		os.Exit(1)
	}

	fromIndex, _, err := loadIndex(filepath.Join(fromDir, "index.json"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail load from index: %v\n", err)
		os.Exit(1)
	}

	walPath := filepath.Join(fromDir, "wal.json")
	if fileExists(walPath) {
		fmt.Println("Found wal.json from previous interrupted run, reconciling...")
		if err := reconcileWAL(walPath, fromDir, toDir, toIndex, toHashSet); err != nil {
			fmt.Fprintf(os.Stderr, "fail reconcile wal: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Reconciliation complete.")
	}

	doneChan := processHashesFromIndex(ctx, fromDir, toDir, fromIndex, toIndex, toHashSet)
	<-doneChan

	fmt.Println("\nMerge completed.")
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
		semconv.ServiceNameKey.String("merge tool"),
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func loadIndex(path string) (map[string]string, map[string]struct{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("fail read %s: %w", path, err)
	}

	var hashes []fileHash
	if err := json.Unmarshal(data, &hashes); err != nil {
		return nil, nil, fmt.Errorf("fail decode %s: %w", path, err)
	}

	index := make(map[string]string, len(hashes))
	hashSet := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		index[h.Path] = h.Hash
		hashSet[h.Hash] = struct{}{}
	}
	return index, hashSet, nil
}

func reconcileWAL(walPath, fromDir, toDir string, toIndex map[string]string, toHashSet map[string]struct{}) error {
	data, err := os.ReadFile(walPath)
	if err != nil {
		return fmt.Errorf("fail read wal: %w", err)
	}

	var wal walEntry
	if err := json.Unmarshal(data, &wal); err != nil {
		return fmt.Errorf("fail decode wal: %w", err)
	}

	return moveWithWAL(fromDir, toDir, wal.Path, wal.Hash, toIndex, toHashSet)
}

func processHashesFromIndex(
	ctx context.Context,
	fromDir, toDir string,
	fromIndex map[string]string,
	toIndex map[string]string,
	toHashSet map[string]struct{},
) chan struct{} {
	doneChan := make(chan struct{})

	go func() {
		_, span := tr.Start(ctx, "processHashesFromIndex")
		defer span.End()

		movedCount := 0
		deletedCount := 0

		for relPath, hash := range fromIndex {
			if _, exists := toHashSet[hash]; exists {
				fullPath := filepath.Join(fromDir, relPath)
				if !fileExists(fullPath) {
					deletedCount++
					fmt.Printf("[DELETED] %s (already missing)\n", relPath)
					continue
				}

				if err := os.Remove(fullPath); err != nil {
					fmt.Fprintf(os.Stderr, "fail delete %s: %v\n", relPath, err)
					continue
				}
				deletedCount++
				fmt.Printf("[DELETED] %s\n", relPath)
			} else {
				err := moveWithWAL(fromDir, toDir, relPath, hash, toIndex, toHashSet)
				if err != nil {
					fmt.Fprintf(os.Stderr, "fail move %s: %v\n", relPath, err)
					continue
				}
				movedCount++
				fmt.Printf("[MOVED] %s\n", relPath)
			}
		}

		fmt.Printf("\nMoved: %d, Deleted: %d\n", movedCount, deletedCount)
		close(doneChan)
	}()

	return doneChan
}

// moveWithWAL выполняет атомарную и возобновляемую операцию перемещения:
// 1. Создаёт wal.json с путём и хешем файла
// 2. Перемещает файл из fromDir в toDir (пропускается, если файл уже перемещён)
// 3. Атомарно обновляет index.json (пропускается, если хеш уже в индексе)
// 4. Удаляет wal.json
//
// При прерывании на любом шаге повторный запуск найдёт wal.json
// и завершит оставшиеся шаги через reconcileWAL → moveWithWAL.
func moveWithWAL(fromDir, toDir, relPath, hash string, toIndex map[string]string, toHashSet map[string]struct{}) error {
	walPath := filepath.Join(fromDir, "wal.json")

	// Шаг 1: создаём wal.json
	wal := walEntry{Path: relPath, Hash: hash}
	walData, err := json.MarshalIndent(wal, "", "  ")
	if err != nil {
		return fmt.Errorf("fail marshal wal: %w", err)
	}
	if err := os.WriteFile(walPath, walData, 0o644); err != nil {
		return fmt.Errorf("fail write wal: %w", err)
	}

	// Шаг 2: перемещаем файл, если он ещё в fromDir
	srcFilePath := filepath.Join(fromDir, relPath)
	if fileExists(srcFilePath) {
		destPath := filepath.Join(toDir, relPath)
		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return fmt.Errorf("fail mkdir: %w", err)
		}
		if err := moveOrCopy(srcFilePath, destPath); err != nil {
			return fmt.Errorf("fail move: %w", err)
		}
		fmt.Printf("[WAL] Moved %s\n", relPath)
	} else {
		fmt.Printf("[WAL] File already moved: %s\n", relPath)
	}

	// Шаг 3: обновляем index.json, если хеш ещё не записан
	if _, exists := toHashSet[hash]; !exists {
		toIndex[relPath] = hash
		toHashSet[hash] = struct{}{}
		if err := saveIndex(toDir, toIndex); err != nil {
			return fmt.Errorf("fail save index: %w", err)
		}
		fmt.Printf("[WAL] Added to index: %s\n", relPath)
	} else {
		fmt.Printf("[WAL] Hash already in index: %s\n", relPath)
	}

	// Шаг 4: удаляем wal.json
	if err := os.Remove(walPath); err != nil {
		return fmt.Errorf("fail remove wal: %w", err)
	}

	return nil
}

func moveOrCopy(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("fail copy: %w", err)
		}
		if err := os.Remove(src); err != nil {
			return fmt.Errorf("fail remove after copy: %w", err)
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func saveIndex(toDir string, toIndex map[string]string) error {
	hashes := make([]fileHash, 0, len(toIndex))
	for path, hash := range toIndex {
		hashes = append(hashes, fileHash{Path: path, Hash: hash})
	}

	data, err := json.MarshalIndent(hashes, "", "  ")
	if err != nil {
		return fmt.Errorf("fail marshal: %w", err)
	}

	indexPath := filepath.Join(toDir, "index.json")
	tmpPath := indexPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("fail write tmp: %w", err)
	}

	if err := os.Rename(tmpPath, indexPath); err != nil {
		return fmt.Errorf("fail rename: %w", err)
	}

	return nil
}
