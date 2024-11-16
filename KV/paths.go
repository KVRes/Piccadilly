package KV

import (
	"path/filepath"
	"strings"
)

func pathToNamespace(path string) string {
	path = strings.ToLower(path)
	elms := strings.Split(path, "/")
	var cleaned []string
	for _, elm := range elms {
		str := strings.TrimSpace(elm)
		str = filterString(str)
		if str != "" {
			cleaned = append(cleaned, str)
		}
	}
	return strings.Join(cleaned, "/")
}

func filterString(str string) string {
	// only allows letters and numbers and _
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, str)
}

func (d *Database) NamespacePath(path string) string {
	path = pathToNamespace(path)
	return filepath.Join(d.basePath, path)
}

func (d *Database) namespacePath(path string) string {
	return filepath.Join(d.basePath, path)
}

func (d *Database) WALPath(path string) string {
	path = pathToNamespace(path)
	return filepath.Join(d.basePath, path, "wal.json")
}

func (d *Database) walPath(path string) string {
	return filepath.Join(d.basePath, path, "wal.json")
}

func (d *Database) PersistPath(path string) string {
	path = pathToNamespace(path)
	return filepath.Join(d.basePath, path, "persist.json")
}

func (d *Database) persistPath(path string) string {
	return filepath.Join(d.basePath, path, "persist.json")
}
