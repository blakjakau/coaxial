package main

import (
	"encoding/json"
	"net/http"
)

// healthHandler answers GET /health with a small JSON status object.
//
// It is used for liveness probes and bootstrapping: the browser client fetches
// this endpoint to obtain the bearer token, then connects to the relay with the
// correct Authorization header. It returns 200 with the configured bearer
// token (empty when auth is disabled).
func healthHandler(bearer string) http.Handler {
	return healthHandlerFunc{bearer: bearer}
}

// healthHandlerFunc is the http.Handler returned by healthHandler.
type healthHandlerFunc struct{ bearer string }

func (h healthHandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"auth":   "enabled",
		"token":  h.bearer,
	})
}
