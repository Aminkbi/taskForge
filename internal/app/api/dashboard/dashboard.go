// Package dashboard serves the embedded TaskForge operator dashboard: a
// self-contained config builder for the TASKFORGE_* environment variables and
// a live operations view backed by the existing /v1/admin endpoints.
package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed assets/*
var assets embed.FS

// Handler returns an http.Handler that serves the dashboard. It is intended to
// be mounted under a prefix (for example "/dashboard/") with that prefix
// stripped beforehand so that the embedded asset paths resolve correctly.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, "assets")
	if err != nil {
		// The embed directive guarantees assets/ exists at build time; a
		// failure here means the binary was built incorrectly.
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
