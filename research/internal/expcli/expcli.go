// Package expcli holds the small process-level helpers shared by the research
// command-line tools: fatal reporting and environment-variable decoding.
package expcli

import (
	"fmt"
	"os"
	"strconv"
)

// Fatal reports a fatal error to stderr and exits with status 1.
func Fatal(format string, args ...any) {
	FatalCode(1, format, args...)
}

// FatalCode reports a fatal error to stderr and exits with the given status.
func FatalCode(code int, format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(code)
}

// Env returns the environment value for key, or fallback when it is unset or empty.
func Env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// EnvInt returns the positive integer environment value for key, or fallback.
func EnvInt(key string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil && value > 0 {
		return value
	}
	return fallback
}
