package kafka

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// Handler exposes the optional management plane. Bind it only to a trusted
// administrative interface; authentication belongs to the surrounding server.
func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /v1/capture", func(w http.ResponseWriter, r *http.Request) { reply(w, http.StatusOK, m.CaptureStatus()) })
	mux.HandleFunc("POST /v1/resync", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Generation string `json:"generation"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			reply(w, http.StatusBadRequest, map[string]string{"error": "invalid resync request"})
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			reply(w, http.StatusBadRequest, map[string]string{"error": "one request object is required"})
			return
		}
		report, err := m.Resync(r.Context(), request.Generation)
		if err != nil {
			code := http.StatusServiceUnavailable
			if errors.Is(err, ErrCaptureConflict) {
				code = http.StatusConflict
			}
			reply(w, code, struct {
				Error   string        `json:"error"`
				Capture CaptureReport `json:"capture"`
			}{err.Error(), report})
			return
		}
		reply(w, http.StatusAccepted, report)
	})
	return mux
}
