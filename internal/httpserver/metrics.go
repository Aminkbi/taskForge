package httpserver

import "net/http"

func registerMetrics(mux *http.ServeMux, handler http.Handler) {
	mux.Handle("/metrics", handler)
}
