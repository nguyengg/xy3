package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chdir cd's into dir for the duration of the subtest, restoring the previous cwd afterwards.
func chdir(t *testing.T, dir string) {
	t.Helper()
	prev, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(prev) })
}

func TestLoader_Load_FindsInCurrentDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".xy3"), []byte("[upload]\nbucket = b\n"), 0o600))
	chdir(t, dir)

	l := &Loader{}
	path, err := l.Load(t.Context())
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(".", ".xy3"), path)
}

func TestLoader_Load_WalksUpToParent(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, ".xy3"), []byte("[upload]\nbucket = b\n"), 0o600))

	child := filepath.Join(root, "a", "b", "c")
	require.NoError(t, os.MkdirAll(child, 0o755))
	chdir(t, child)

	l := &Loader{}
	path, err := l.Load(t.Context())
	require.NoError(t, err)
	assert.NotEqual(t, "", path)
	assert.Equal(t, filepath.Base(path), ".xy3")
}

// TestLoader_Load_DirectoryNamedXy3_DoesNotHang is the H2 regression test:
// before the fix, a directory literally named ".xy3" made Load spin forever on
// os.Stat of the same path. This test ensures the loop advances past it.
func TestLoader_Load_DirectoryNamedXy3_DoesNotHang(t *testing.T) {
	root := t.TempDir()

	child := filepath.Join(root, "leaf")
	require.NoError(t, os.MkdirAll(child, 0o755))
	// place a DIRECTORY named .xy3 in the child so the loop must skip past it.
	require.NoError(t, os.MkdirAll(filepath.Join(child, ".xy3"), 0o755))
	chdir(t, child)

	// Load must terminate — cap it with a short deadline to catch a regression as a failing test
	// instead of a hung run.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	l := &Loader{}
	// no .xy3 file exists anywhere up the tree, so a non-hang Load should return ("", nil).
	// (before the fix this hung forever until the ctx deadline fired.)
	path, err := l.Load(ctx)
	require.NoError(t, err, "Load must not hang on a directory named .xy3")
	assert.Equal(t, "", path)
}

func TestLoader_Load_NoConfigFound(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	l := &Loader{}
	path, err := l.Load(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "", path)
}

func TestLoader_Load_ContextCancelled(t *testing.T) {
	dir := t.TempDir()
	chdir(t, dir)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	l := &Loader{}
	_, err := l.Load(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}
