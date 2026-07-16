package httpserver

import "net/http"

func registerMetrics(mux *http.ServeMux, handler http.Handler) {
	if handler == nil {
		handler = http.NotFoundHandler()
	}
	mux.Handle("/metrics", ReadOnly(handler))
}
