package pkg

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ClusterHashTagForKey returns the Redis Cluster hashtag for a key.
func ClusterHashTagForKey(key string) string {
	s := strings.IndexByte(key, '{')
	if s >= 0 {
		e := strings.IndexByte(key[s+1:], '}')
		if e >= 0 {
			t := key[s+1 : s+1+e]
			if t != "" {
				return t
			}
		}
	}
	return key
}

// findRepoRoot walks up from the current working directory to locate
// the repository root containing the deployments' folder.
func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "deployments")); statErr == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	return "", fmt.Errorf("repository root not found")
}
