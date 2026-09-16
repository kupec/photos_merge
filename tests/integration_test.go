package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// fileHash mirrors the struct from init.go / merge.go.
type fileHash struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

// walEntry mirrors the struct from merge.go.
type walEntry struct {
	Path string `json:"path"`
	Hash string `json:"hash"`
}

var (
	initBinary  string
	mergeBinary string
)

// TestMain builds the init and merge binaries once before all tests.
func TestMain(m *testing.M) {
	// Find the module root via go env GOMOD.
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "go env GOMOD failed: %v\n", err)
		os.Exit(1)
	}
	gomod := strings.TrimSpace(string(out))
	if gomod == "" || gomod == "/dev/null" {
		fmt.Fprintln(os.Stderr, "not inside a Go module")
		os.Exit(1)
	}
	moduleRoot := filepath.Dir(gomod)

	// Source directories — override via env vars if your layout differs.
	initSrc := os.Getenv("TEST_INIT_SRC")
	if initSrc == "" {
		initSrc = "./init.go"
	}
	mergeSrc := os.Getenv("TEST_MERGE_SRC")
	if mergeSrc == "" {
		mergeSrc = "./merge.go"
	}

	// Temp dir for compiled binaries.
	buildDir, err := os.MkdirTemp("", "photoidx-build-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "MkdirTemp: %v\n", err)
		os.Exit(1)
	}

	initBinary = filepath.Join(buildDir, "init")
	mergeBinary = filepath.Join(buildDir, "merge")

	// Build init.
	cmd := exec.Command("go", "build", "-o", initBinary, initSrc)
	cmd.Dir = moduleRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build init (%s) failed: %v\n%s\n", initSrc, err, out)
		os.Exit(1)
	}

	// Build merge.
	cmd = exec.Command("go", "build", "-o", mergeBinary, mergeSrc)
	cmd.Dir = moduleRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build merge (%s) failed: %v\n%s\n", mergeSrc, err, out)
		os.Exit(1)
	}

	code := m.Run()
	_ = os.RemoveAll(buildDir)
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// sha256hex computes the SHA-256 hex digest — same algorithm as init.go.
func sha256hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// createTempDir creates a temp directory and registers cleanup.
func createTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "photoidx-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// writeFile creates a file inside dir with the given relative name and content.
// Parent directories are created as needed.
func writeFile(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	full := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(full), err)
	}
	if err := os.WriteFile(full, content, 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", full, err)
	}
}

// runInit executes the init binary on dir (passing "." so paths are relative).
func runInit(t *testing.T, dir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, initBinary, ".")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init failed: %v\n%s", err, out)
	}
}

// runMerge executes the merge binary: moves files from fromDir into toDir.
func runMerge(t *testing.T, fromDir, toDir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, mergeBinary, fromDir, toDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("merge failed: %v\n%s", err, out)
	}
}

// loadIndexMap reads index.json from dir and returns a path→hash map.
func loadIndexMap(t *testing.T, dir string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil {
		t.Fatalf("ReadFile index.json in %s: %v", dir, err)
	}
	var hashes []fileHash
	if err := json.Unmarshal(data, &hashes); err != nil {
		t.Fatalf("Unmarshal index.json: %v", err)
	}
	m := make(map[string]string, len(hashes))
	for _, h := range hashes {
		m[h.Path] = h.Hash
	}
	return m
}

// assertIndexEquals checks that index.json in dir matches expected.
func assertIndexEquals(t *testing.T, dir string, expected map[string]string) {
	t.Helper()
	actual := loadIndexMap(t, dir)
	if len(actual) != len(expected) {
		t.Errorf("index size mismatch: got %d, want %d", len(actual), len(expected))
	}
	for path, hash := range expected {
		got, ok := actual[path]
		if !ok {
			t.Errorf("index missing path %q", path)
			continue
		}
		if got != hash {
			t.Errorf("hash mismatch for %q: got %s, want %s", path, got, hash)
		}
	}
	for path := range actual {
		if _, ok := expected[path]; !ok {
			t.Errorf("index has unexpected path %q", path)
		}
	}
}

// assertFileExists asserts that a file exists at path.
func assertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("expected file %q to exist: %v", path, err)
	}
}

// assertFileNotExists asserts that no file exists at path.
func assertFileNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Errorf("expected file %q to not exist", path)
	}
}

// partialIndexRegex matches index.<digits>.json (same pattern the scripts use).
var partialIndexRegex = regexp.MustCompile(`index\.\d+\.json`)

// assertNoPartialIndexes checks that no index.N.json files remain in dir.
func assertNoPartialIndexes(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	for _, e := range entries {
		if partialIndexRegex.MatchString(e.Name()) {
			t.Errorf("partial index file not cleaned up: %s", e.Name())
		}
	}
}

// ---------------------------------------------------------------------------
// Init tests
// ---------------------------------------------------------------------------

// TestInit_EmptyDir verifies that running init on an empty directory
// produces an empty index.json.
func TestInit_EmptyDir(t *testing.T) {
	dir := createTempDir(t)
	runInit(t, dir)

	index := loadIndexMap(t, dir)
	if len(index) != 0 {
		t.Errorf("expected empty index, got %d entries", len(index))
	}
}

// TestInit_BasicFiles checks that init correctly hashes multiple files
// and stores their paths and SHA-256 hashes in index.json.
func TestInit_BasicFiles(t *testing.T) {
	dir := createTempDir(t)

	contentA := []byte("content of file A")
	contentB := []byte("content of file B")
	contentC := []byte("content of file C")

	writeFile(t, dir, "a.txt", contentA)
	writeFile(t, dir, "b.txt", contentB)
	writeFile(t, dir, "c.txt", contentC)

	runInit(t, dir)

	expected := map[string]string{
		"a.txt": sha256hex(contentA),
		"b.txt": sha256hex(contentB),
		"c.txt": sha256hex(contentC),
	}
	assertIndexEquals(t, dir, expected)
	assertNoPartialIndexes(t, dir)
}

// TestInit_NestedDirs verifies that init correctly handles files
// inside subdirectories.
func TestInit_NestedDirs(t *testing.T) {
	dir := createTempDir(t)

	content1 := []byte("nested file 1")
	content2 := []byte("nested file 2")
	content3 := []byte("root file")

	writeFile(t, dir, "sub1/file1.txt", content1)
	writeFile(t, dir, "sub1/deep/file2.txt", content2)
	writeFile(t, dir, "root.txt", content3)

	runInit(t, dir)

	expected := map[string]string{
		"sub1/file1.txt":      sha256hex(content1),
		"sub1/deep/file2.txt": sha256hex(content2),
		"root.txt":            sha256hex(content3),
	}
	assertIndexEquals(t, dir, expected)
}

// TestInit_Resume simulates a previously interrupted run: a partial
// index.0.json already contains hashes for some files. Init should
// load those, skip re-hashing them, hash only new files, and produce
// a complete index.json.
func TestInit_Resume(t *testing.T) {
	dir := createTempDir(t)

	contentA := []byte("file A content")
	contentB := []byte("file B content")
	contentC := []byte("file C content")

	writeFile(t, dir, "a.txt", contentA)
	writeFile(t, dir, "b.txt", contentB)
	writeFile(t, dir, "c.txt", contentC)

	// Pre-create a partial index with a.txt and b.txt already hashed.
	partial := []fileHash{
		{Path: "a.txt", Hash: sha256hex(contentA)},
		{Path: "b.txt", Hash: sha256hex(contentB)},
	}
	partialData, _ := json.MarshalIndent(partial, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "index.0.json"), partialData, 0o644); err != nil {
		t.Fatalf("WriteFile partial index: %v", err)
	}

	runInit(t, dir)

	expected := map[string]string{
		"a.txt": sha256hex(contentA),
		"b.txt": sha256hex(contentB),
		"c.txt": sha256hex(contentC),
	}
	assertIndexEquals(t, dir, expected)
	assertNoPartialIndexes(t, dir)
}

// TestInit_SecondRun verifies that running init twice on the same
// directory produces the same index.json both times.
func TestInit_SecondRun(t *testing.T) {
	dir := createTempDir(t)

	contentA := []byte("file A")
	contentB := []byte("file B")

	writeFile(t, dir, "a.txt", contentA)
	writeFile(t, dir, "b.txt", contentB)

	expected := map[string]string{
		"a.txt": sha256hex(contentA),
		"b.txt": sha256hex(contentB),
	}

	runInit(t, dir)
	assertIndexEquals(t, dir, expected)

	// Run again — result should be identical.
	runInit(t, dir)
	assertIndexEquals(t, dir, expected)
	assertNoPartialIndexes(t, dir)
}

// TestInit_IdenticalContent verifies that two files with the same
// content but different names get the same hash but different paths.
func TestInit_IdenticalContent(t *testing.T) {
	dir := createTempDir(t)

	content := []byte("identical content")
	writeFile(t, dir, "copy1.txt", content)
	writeFile(t, dir, "copy2.txt", content)

	runInit(t, dir)

	h := sha256hex(content)
	expected := map[string]string{
		"copy1.txt": h,
		"copy2.txt": h,
	}
	assertIndexEquals(t, dir, expected)
}

// TestInit_ManyFiles creates more files than partSize (100) to verify
// that intermediate index.N.json parts are created and then cleaned up,
// leaving only the final index.json with all entries.
func TestInit_ManyFiles(t *testing.T) {
	dir := createTempDir(t)

	const n = 105 // > partSize (100)
	expected := make(map[string]string, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("file_%03d.txt", i)
		content := []byte(fmt.Sprintf("content %d", i))
		writeFile(t, dir, name, content)
		expected[name] = sha256hex(content)
	}

	runInit(t, dir)

	assertIndexEquals(t, dir, expected)
	assertNoPartialIndexes(t, dir)
}

// ---------------------------------------------------------------------------
// Merge tests
// ---------------------------------------------------------------------------

// TestMerge_NoDuplicates merges two disjoint directories: all files from
// fromDir should be moved into toDir, and toDir's index.json should
// contain all entries from both directories.
func TestMerge_NoDuplicates(t *testing.T) {
	fromDir := createTempDir(t)
	toDir := createTempDir(t)

	contentA := []byte("file A")
	contentB := []byte("file B")
	contentX := []byte("file X")
	contentY := []byte("file Y")

	writeFile(t, fromDir, "a.txt", contentA)
	writeFile(t, fromDir, "b.txt", contentB)
	writeFile(t, toDir, "x.txt", contentX)
	writeFile(t, toDir, "y.txt", contentY)

	runInit(t, fromDir)
	runInit(t, toDir)
	runMerge(t, fromDir, toDir)

	// Files moved to toDir.
	assertFileExists(t, filepath.Join(toDir, "a.txt"))
	assertFileExists(t, filepath.Join(toDir, "b.txt"))
	assertFileExists(t, filepath.Join(toDir, "x.txt"))
	assertFileExists(t, filepath.Join(toDir, "y.txt"))

	// Files removed from fromDir.
	assertFileNotExists(t, filepath.Join(fromDir, "a.txt"))
	assertFileNotExists(t, filepath.Join(fromDir, "b.txt"))

	// Merged index has all 4 entries.
	expected := map[string]string{
		"a.txt": sha256hex(contentA),
		"b.txt": sha256hex(contentB),
		"x.txt": sha256hex(contentX),
		"y.txt": sha256hex(contentY),
	}
	assertIndexEquals(t, toDir, expected)
}

// TestMerge_WithDuplicates merges two directories where some files in
// fromDir have the same content (and thus hash) as files in toDir.
// Duplicates should be deleted from fromDir, not moved.
func TestMerge_WithDuplicates(t *testing.T) {
	fromDir := createTempDir(t)
	toDir := createTempDir(t)

	contentA := []byte("unique A")
	contentB := []byte("unique B")
	contentDup := []byte("duplicate content")
	contentX := []byte("file X")

	writeFile(t, fromDir, "a.txt", contentA)
	writeFile(t, fromDir, "b.txt", contentB)
	writeFile(t, fromDir, "dup.txt", contentDup)
	writeFile(t, toDir, "existing_dup.txt", contentDup)
	writeFile(t, toDir, "x.txt", contentX)

	runInit(t, fromDir)
	runInit(t, toDir)
	runMerge(t, fromDir, toDir)

	// Unique files moved to toDir.
	assertFileExists(t, filepath.Join(toDir, "a.txt"))
	assertFileExists(t, filepath.Join(toDir, "b.txt"))

	// Duplicate deleted from fromDir, NOT moved to toDir.
	assertFileNotExists(t, filepath.Join(fromDir, "dup.txt"))
	assertFileNotExists(t, filepath.Join(fromDir, "a.txt"))
	assertFileNotExists(t, filepath.Join(fromDir, "b.txt"))
	assertFileNotExists(t, filepath.Join(toDir, "dup.txt"))

	// Original toDir file still present.
	assertFileExists(t, filepath.Join(toDir, "existing_dup.txt"))
	assertFileExists(t, filepath.Join(toDir, "x.txt"))

	// Index has a.txt, b.txt, existing_dup.txt, x.txt (NOT dup.txt).
	expected := map[string]string{
		"a.txt":            sha256hex(contentA),
		"b.txt":            sha256hex(contentB),
		"existing_dup.txt": sha256hex(contentDup),
		"x.txt":            sha256hex(contentX),
	}
	assertIndexEquals(t, toDir, expected)
}

// TestMerge_WALReconciliation simulates an interrupted merge by
// creating a wal.json file in fromDir. The next merge run should
// detect the WAL, complete the interrupted operation, and clean up.
func TestMerge_WALReconciliation(t *testing.T) {
	fromDir := createTempDir(t)
	toDir := createTempDir(t)

	contentA := []byte("file A")
	contentB := []byte("file B")
	contentX := []byte("file X")

	writeFile(t, fromDir, "a.txt", contentA)
	writeFile(t, fromDir, "b.txt", contentB)
	writeFile(t, toDir, "x.txt", contentX)

	runInit(t, fromDir)
	runInit(t, toDir)

	// Simulate interrupted merge: create wal.json for a.txt
	// (as if merge was interrupted after writing WAL but before moving file).
	wal := walEntry{Path: "a.txt", Hash: sha256hex(contentA)}
	walData, _ := json.MarshalIndent(wal, "", "  ")
	if err := os.WriteFile(filepath.Join(fromDir, "wal.json"), walData, 0o644); err != nil {
		t.Fatalf("WriteFile wal.json: %v", err)
	}

	runMerge(t, fromDir, toDir)

	// WAL should be cleaned up.
	assertFileNotExists(t, filepath.Join(fromDir, "wal.json"))

	// All files should end up in toDir.
	assertFileExists(t, filepath.Join(toDir, "a.txt"))
	assertFileExists(t, filepath.Join(toDir, "b.txt"))
	assertFileExists(t, filepath.Join(toDir, "x.txt"))

	// fromDir should be empty of data files.
	assertFileNotExists(t, filepath.Join(fromDir, "a.txt"))
	assertFileNotExists(t, filepath.Join(fromDir, "b.txt"))

	// Merged index.
	expected := map[string]string{
		"a.txt": sha256hex(contentA),
		"b.txt": sha256hex(contentB),
		"x.txt": sha256hex(contentX),
	}
	assertIndexEquals(t, toDir, expected)
}

// TestMerge_WALReconciliation_AfterMove simulates an interruption that
// occurred after the file was moved but before the index was updated.
// The reconciliation should update the index and remove the WAL.
func TestMerge_WALReconciliation_AfterMove(t *testing.T) {
	fromDir := createTempDir(t)
	toDir := createTempDir(t)

	contentA := []byte("file A")
	contentB := []byte("file B")
	contentX := []byte("file X")

	writeFile(t, fromDir, "a.txt", contentA)
	writeFile(t, fromDir, "b.txt", contentB)
	writeFile(t, toDir, "x.txt", contentX)

	runInit(t, fromDir)
	runInit(t, toDir)

	// Simulate: a.txt was already moved to toDir, but index was not
	// updated and wal.json was not removed.
	if err := os.Rename(filepath.Join(fromDir, "a.txt"), filepath.Join(toDir, "a.txt")); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	wal := walEntry{Path: "a.txt", Hash: sha256hex(contentA)}
	walData, _ := json.MarshalIndent(wal, "", "  ")
	if err := os.WriteFile(filepath.Join(fromDir, "wal.json"), walData, 0o644); err != nil {
		t.Fatalf("WriteFile wal.json: %v", err)
	}

	runMerge(t, fromDir, toDir)

	// WAL cleaned up.
	assertFileNotExists(t, filepath.Join(fromDir, "wal.json"))

	// a.txt is in toDir (was already there).
	assertFileExists(t, filepath.Join(toDir, "a.txt"))
	// b.txt moved to toDir.
	assertFileExists(t, filepath.Join(toDir, "b.txt"))
	// x.txt still there.
	assertFileExists(t, filepath.Join(toDir, "x.txt"))

	// fromDir empty of data files.
	assertFileNotExists(t, filepath.Join(fromDir, "a.txt"))
	assertFileNotExists(t, filepath.Join(fromDir, "b.txt"))

	// Index updated with all entries.
	expected := map[string]string{
		"a.txt": sha256hex(contentA),
		"b.txt": sha256hex(contentB),
		"x.txt": sha256hex(contentX),
	}
	assertIndexEquals(t, toDir, expected)
}

// TestMerge_EmptyFromDir merges from an empty directory (only index.json,
// no data files). Nothing should be moved; toDir should be unchanged.
func TestMerge_EmptyFromDir(t *testing.T) {
	fromDir := createTempDir(t)
	toDir := createTempDir(t)

	contentX := []byte("file X")
	writeFile(t, toDir, "x.txt", contentX)

	runInit(t, fromDir)
	runInit(t, toDir)
	runMerge(t, fromDir, toDir)

	// toDir unchanged.
	assertFileExists(t, filepath.Join(toDir, "x.txt"))
	expected := map[string]string{
		"x.txt": sha256hex(contentX),
	}
	assertIndexEquals(t, toDir, expected)
}
