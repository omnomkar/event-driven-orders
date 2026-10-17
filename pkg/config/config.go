// Package config reads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Loader reads environment variables and collects every problem it finds,
// so a misconfigured service reports all missing or invalid values at once.
type Loader struct {
	lookup func(string) (string, bool)
	errs   []error
}

// New returns a Loader that reads the process environment.
func New() *Loader {
	return NewWithLookup(os.LookupEnv)
}

// NewWithLookup returns a Loader that reads values through lookup. Tests use it
// to supply a fake environment.
func NewWithLookup(lookup func(string) (string, bool)) *Loader {
	return &Loader{lookup: lookup}
}

// get returns the trimmed value of key. An empty value counts as unset.
func (l *Loader) get(key string) (string, bool) {
	v, ok := l.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

// String returns the value of key, or def if it is unset.
func (l *Loader) String(key, def string) string {
	if v, ok := l.get(key); ok {
		return v
	}
	return def
}

// Required returns the value of key and records an error if it is unset.
func (l *Loader) Required(key string) string {
	v, ok := l.get(key)
	if !ok {
		l.errs = append(l.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

// Int returns key parsed as an integer, or def if it is unset.
func (l *Loader) Int(key string, def int) int {
	v, ok := l.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

// Bool returns key parsed with strconv.ParseBool ("true", "false", "1",
// "0", and so on), or def if it is unset.
func (l *Loader) Bool(key string, def bool) bool {
	v, ok := l.get(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

// Duration returns key parsed with time.ParseDuration (for example "10s"),
// or def if it is unset.
func (l *Loader) Duration(key string, def time.Duration) time.Duration {
	v, ok := l.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		l.errs = append(l.errs, fmt.Errorf("%s: %q is not a positive duration", key, v))
		return def
	}
	return d
}

// RequiredList returns key split on commas with blanks removed, and records
// an error if it is unset or contains no items.
func (l *Loader) RequiredList(key string) []string {
	v, _ := l.get(key)
	var items []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			items = append(items, s)
		}
	}
	if len(items) == 0 {
		l.errs = append(l.errs, fmt.Errorf("%s is required", key))
	}
	return items
}

// Err returns every problem found so far joined into one error, or nil.
func (l *Loader) Err() error {
	return errors.Join(l.errs...)
}
