package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"
)

type ctxKey struct{}

// reqInfo is filled in by handlers and read back by the access-log middleware.
type reqInfo struct {
	id      string
	userID  string
	outcome string
	log     *slog.Logger
}

func info(r *http.Request) *reqInfo {
	if ri, ok := r.Context().Value(ctxKey{}).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{log: slog.Default()}
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// observe assigns a correlation id, recovers panics, records metrics and
// writes one structured access-log line per request.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-ID")
		if !requestIDPattern.MatchString(id) {
			id = newID()
		}
		ri := &reqInfo{id: id, log: s.log.With("request_id", id)}
		r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, ri))
		w.Header().Set("X-Request-ID", id)
		rec := &statusRecorder{ResponseWriter: w}

		s.m.InFlight.Inc()
		defer func() {
			s.m.InFlight.Dec()
			if p := recover(); p != nil {
				ri.log.Error("panic", "panic", p, "stack", string(debug.Stack()))
				if rec.status == 0 {
					writeJSON(rec, http.StatusInternalServerError, errBody("internal_error", "unexpected error"))
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			dur := time.Since(start)
			s.m.HTTPRequests.WithLabelValues(r.Method, route, itoa(status)).Inc()
			s.m.HTTPDuration.WithLabelValues(r.Method, route).Observe(dur.Seconds())

			if route == "GET /metrics" || route == "GET /healthz" || route == "GET /readyz" {
				return // keep probe/scrape noise out of the log
			}
			level := slog.LevelInfo
			if status >= 500 {
				level = slog.LevelError
			}
			attrs := []any{
				"method", r.Method, "route", route, "path", r.URL.Path,
				"status", status, "duration_ms", float64(dur.Microseconds()) / 1000,
			}
			if ri.userID != "" {
				attrs = append(attrs, "user_id", ri.userID)
			}
			if ri.outcome != "" {
				attrs = append(attrs, "outcome", ri.outcome)
			}
			ri.log.Log(r.Context(), level, "request", attrs...)
		}()
		next.ServeHTTP(rec, r)
	})
}

func itoa(n int) string {
	if n >= 100 && n < 1000 {
		return string([]byte{byte('0' + n/100), byte('0' + n/10%10), byte('0' + n%10)})
	}
	return "other"
}
