// Command healthcheck checks the sidecar's local health endpoint. It is kept
// separate from the service binaries so minimal distroless images need no shell
// or curl for Docker HEALTHCHECK.
package main

import (
	"net/http"
	"os"
	"time"
)

func main() {
	url := os.Getenv("TASKFORGE_HEALTHCHECK_URL")
	if url == "" {
		url = "http://127.0.0.1:8080/healthz"
	}
	client := http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url)
	if err != nil || response.StatusCode != http.StatusOK {
		if response != nil {
			response.Body.Close()
		}
		os.Exit(1)
	}
	response.Body.Close()
}
