package arrowipc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const arrowIPCImport = "github.com/apache/arrow-go/v18/arrow/ipc"

// readerConstructors are the arrow-go calls that build something which reads
// Arrow IPC bytes. All of them are given the size a message states for itself
// unless the caller says otherwise, and the bytes of InsertRows come from a
// client.
var readerConstructors = map[string]bool{
	"NewReader":                  true,
	"NewReaderFromMessageReader": true,
	"NewMessageReader":           true,
	"NewFileReader":              true,
	"NewFileReaderFromMemory":    true,
}

// directReaders lists the arrow-go reader constructors a source file calls.
func directReaders(t *testing.T, name string, src any) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	require.NoError(t, err, name)

	local := ""
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		require.NoError(t, err)
		if path != arrowIPCImport {
			continue
		}
		local = "ipc"
		if imp.Name != nil {
			local = imp.Name.Name
		}
	}
	if local == "" {
		return nil
	}
	var found []string
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == local && readerConstructors[sel.Sel.Name] {
			found = append(found, sel.Sel.Name)
		}
		return true
	})
	return found
}

func TestTheDetectorSeesADirectReader(t *testing.T) {
	src := `package x
import arrowipc "github.com/apache/arrow-go/v18/arrow/ipc"
func f() { _, _ = arrowipc.NewReader(nil) }`
	require.Equal(t, []string{"NewReader"}, directReaders(t, "x.go", src))
	require.Empty(t, directReaders(t, "y.go", `package y
import "github.com/apache/arrow-go/v18/arrow/ipc"
func f() { _ = ipc.NewWriter }`), "writers are not readers")
}

// A reader built anywhere but here does not have the size limits. This fails the
// day any other file, test files included, builds one.
func TestNoReaderIsBuiltOutsideThisPackage(t *testing.T) {
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			if rel == "gen" || d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || rel == filepath.Join("internal", "arrowipc", "arrowipc.go") {
			return nil
		}
		for _, call := range directReaders(t, path, nil) {
			t.Errorf("%s calls ipc.%s: build the reader with arrowipc.NewReader, which applies the message size limits", rel, call)
		}
		return nil
	})
	require.NoError(t, err)
}
