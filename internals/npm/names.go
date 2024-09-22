package npm

import (
	"strings"
)

const (
	ScopedPackagePrefix = "@"
)

func IsScopedPackageName(name string) bool {
	return strings.HasPrefix(name, ScopedPackagePrefix)
}
