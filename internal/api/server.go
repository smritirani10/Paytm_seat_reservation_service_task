// Package api is the JSON HTTP layer. It authenticates, validates input and
// maps store outcomes to status codes; it makes no seat decisions itself.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/auth"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/metrics"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/store"
)

const (
	defaultPerUserLimit = 4
	maxSeatsPerShow     = 100_000
	maxSeatsPerRequest  = 20
	maxLabelLen         = 32
	maxIdemKeyLen       = 200
	dbTimeout           = 30 * time.Second
)

type Server struct {
	st    *store.Store
	auth  *auth.Authenticator
	m     *metrics.Metrics
	log   *slog.Logger
	ready *atomic.Bool
}

func New(st *store.Store, a *auth.Authenticator, m *metrics.Metrics, log *slog.Logger, ready *atomic.Bool) *Server {
	return &Server{st: st, auth: a, m: m, log: log, ready: ready}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.m.Registry, promhttp.HandlerOpts{}))

	mux.HandleFunc("POST /auth/token", s.issueToken)

	mux.HandleFunc("POST /shows", s.createShow)
	mux.HandleFunc("GET /shows/{id}", s.getShow)
	mux.HandleFunc("GET /shows/{id}/audit", s.auditShow)
	mux.HandleFunc("POST /shows/{id}/reserve", s.reserve)

	mux.HandleFunc("GET /reservations/{id}", s.getReservation)
	mux.HandleFunc("POST /reservations/{id}/cancel", s.cancel)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, errBody("not_found", "no such route"))
	})
	return s.observe(mux)
}

// dbCtx detaches DB work from client disconnects so a transaction that has
// started always runs to a definite commit/rollback, bounded by dbTimeout.
func dbCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), dbTimeout)
}

// ---------------------------------------------------------------- health

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "seat-reservation",
		"endpoints": []string{
			"POST /auth/token", "POST /shows (admin)", "GET /shows/{id}",
			"GET /shows/{id}/audit", "POST /shows/{id}/reserve",
			"GET /reservations/{id}", "POST /reservations/{id}/cancel",
			"GET /healthz", "GET /readyz", "GET /metrics",
		},
	})
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz fails closed: not ready until migrations ran, and not ready whenever
// the database doesn't answer within a second.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting", "db": "not_migrated"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.st.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "db": "unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "db": "ok"})
}

// ---------------------------------------------------------------- auth

func (s *Server) issueToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
	}
	if !decode(w, r, &req, 4<<10) {
		return
	}
	if !auth.ValidUserID(req.UserID) {
		writeJSON(w, http.StatusBadRequest, errBody("invalid_user_id", "user_id must match [A-Za-z0-9_.:@-]{1,128}"))
		return
	}
	tok, exp, err := s.auth.Issue(req.UserID)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": tok, "user_id": req.UserID, "expires_at": exp.UTC()})
}

func (s *Server) requireUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	uid, err := s.auth.UserFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errBody("unauthorized", err.Error()))
		return "", false
	}
	info(r).userID = uid
	return uid, true
}

// ---------------------------------------------------------------- shows

func (s *Server) createShow(w http.ResponseWriter, r *http.Request) {
	if !s.auth.IsAdmin(r) {
		writeJSON(w, http.StatusUnauthorized, errBody("unauthorized", "admin token required"))
		return
	}
	var req struct {
		Name         string   `json:"name"`
		Seats        []string `json:"seats"`
		PricePaise   *int64   `json:"price_paise"`
		PerUserLimit *int     `json:"per_user_limit"`
	}
	if !decode(w, r, &req, 8<<20) {
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	switch {
	case req.Name == "" || len(req.Name) > 200:
		writeJSON(w, http.StatusBadRequest, errBody("invalid_name", "name is required (max 200 chars)"))
		return
	case len(req.Seats) == 0 || len(req.Seats) > maxSeatsPerShow:
		writeJSON(w, http.StatusBadRequest, errBody("invalid_seats", "seats must be a non-empty list (max 100000)"))
		return
	case req.PricePaise == nil || *req.PricePaise < 0:
		writeJSON(w, http.StatusBadRequest, errBody("invalid_price", "price_paise must be a non-negative integer"))
		return
	}
	limit := defaultPerUserLimit
	if req.PerUserLimit != nil {
		if *req.PerUserLimit < 1 || *req.PerUserLimit > 1000 {
			writeJSON(w, http.StatusBadRequest, errBody("invalid_per_user_limit", "per_user_limit must be 1..1000"))
			return
		}
		limit = *req.PerUserLimit
	}
	seen := make(map[string]struct{}, len(req.Seats))
	for _, l := range req.Seats {
		if !validLabel(l) {
			writeJSON(w, http.StatusBadRequest, errBody("invalid_seats", "invalid seat label: "+truncate(l)))
			return
		}
		if _, dup := seen[l]; dup {
			writeJSON(w, http.StatusBadRequest, errBody("invalid_seats", "duplicate seat label: "+l))
			return
		}
		seen[l] = struct{}{}
	}
	ctx, cancel := dbCtx(r)
	defer cancel()
	show, err := s.st.CreateShow(ctx, req.Name, req.Seats, *req.PricePaise, limit)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	info(r).log.Info("show created", "show_id", show.ID, "seats", show.TotalSeats, "price_paise", show.PricePaise, "per_user_limit", show.PerUserLimit)
	writeJSON(w, http.StatusCreated, show)
}

func (s *Server) getShow(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := dbCtx(r)
	defer cancel()
	st, err := s.st.GetShowState(ctx, r.PathValue("id"))
	if errors.Is(err, store.ErrShowNotFound) {
		writeJSON(w, http.StatusNotFound, errBody("show_not_found", "no such show"))
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) auditShow(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := dbCtx(r)
	defer cancel()
	a, err := s.st.AuditShow(ctx, r.PathValue("id"))
	if errors.Is(err, store.ErrShowNotFound) {
		writeJSON(w, http.StatusNotFound, errBody("show_not_found", "no such show"))
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	if !a.OK {
		info(r).log.Error("AUDIT FAILED", "show_id", a.ShowID, "audit", a)
	}
	writeJSON(w, http.StatusOK, a)
}

// ---------------------------------------------------------------- reserve

func (s *Server) reserve(w http.ResponseWriter, r *http.Request) {
	ri := info(r)
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	// Unknown fields (e.g. a spoofed "user_id") are ignored: identity only
	// ever comes from the token.
	var req struct {
		Seats          []string `json:"seats"`
		IdempotencyKey string   `json:"idempotency_key"`
	}
	if !decode(w, r, &req, 64<<10) {
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	switch {
	case key != "" && req.IdempotencyKey != "" && key != req.IdempotencyKey:
		writeJSON(w, http.StatusBadRequest, errBody("invalid_idempotency_key", "Idempotency-Key header and body idempotency_key differ"))
		return
	case key == "":
		key = req.IdempotencyKey
	}
	if len(key) > maxIdemKeyLen {
		writeJSON(w, http.StatusBadRequest, errBody("invalid_idempotency_key", "idempotency key too long (max 200)"))
		return
	}
	if key == "" {
		// No key supplied: the request is simply not retry-safe.
		key = "auto:" + newID() + newID()
	}
	if len(req.Seats) == 0 || len(req.Seats) > maxSeatsPerRequest {
		writeJSON(w, http.StatusBadRequest, errBody("invalid_seats", "seats must be a non-empty list (max 20)"))
		return
	}
	seen := make(map[string]struct{}, len(req.Seats))
	for _, l := range req.Seats {
		if !validLabel(l) {
			writeJSON(w, http.StatusBadRequest, errBody("invalid_seats", "invalid seat label: "+truncate(l)))
			return
		}
		if _, dup := seen[l]; dup {
			writeJSON(w, http.StatusBadRequest, errBody("invalid_seats", "duplicate seat in request: "+l))
			return
		}
		seen[l] = struct{}{}
	}

	ctx, cancel := dbCtx(r)
	defer cancel()
	show, err := s.st.GetShowMeta(ctx, r.PathValue("id"))
	if errors.Is(err, store.ErrShowNotFound) {
		writeJSON(w, http.StatusNotFound, errBody("show_not_found", "no such show"))
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}

	res, err := s.st.Reserve(ctx, show, uid, req.Seats, key)
	if err != nil {
		s.m.ReserveErrors.Inc()
		s.internal(w, r, err)
		return
	}
	if res.FastPath {
		s.m.FastPathDecisions.WithLabelValues(outcomeName(res.Outcome)).Inc()
	}
	ri.outcome = outcomeName(res.Outcome)

	switch res.Outcome {
	case store.OutcomeConfirmed:
		s.m.ReservationsConfirmed.WithLabelValues(show.ID).Inc()
		s.m.SeatsConfirmed.WithLabelValues(show.ID).Add(float64(len(res.Reservation.Seats)))
		ri.log.Info("reservation confirmed", "show_id", show.ID, "reservation_id", res.Reservation.ID, "seats", res.Reservation.Seats, "user_id", uid)
		writeJSON(w, http.StatusCreated, res.Reservation)
	case store.OutcomeReplayed:
		s.m.ReservationsDeclined.WithLabelValues(show.ID, metrics.ReasonIdempotentReplay).Inc()
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, res.Reservation)
	case store.OutcomeIdempotencyConflict:
		s.m.ReservationsDeclined.WithLabelValues(show.ID, metrics.ReasonIdempotencyConflict).Inc()
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":                   "idempotency_key_conflict",
			"message":                 "this idempotency key was already used with a different request body",
			"original_reservation_id": res.Reservation.ID,
		})
	case store.OutcomeSeatTaken:
		s.m.ReservationsDeclined.WithLabelValues(show.ID, metrics.ReasonSeatTaken).Inc()
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":   "seat_taken",
			"message": "one or more requested seats are already taken; nothing was reserved",
			"seats":   res.Seats,
		})
	case store.OutcomeLimitExceeded:
		s.m.ReservationsDeclined.WithLabelValues(show.ID, metrics.ReasonPerUserLimit).Inc()
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":          "per_user_limit_exceeded",
			"message":        "reservation would exceed the per-user seat limit for this show",
			"per_user_limit": res.Limit,
			"already_held":   res.Held,
			"requested":      len(req.Seats),
		})
	case store.OutcomeUnknownSeats:
		s.m.ReservationsDeclined.WithLabelValues(show.ID, metrics.ReasonUnknownSeat).Inc()
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   "unknown_seats",
			"message": "requested seats do not exist in this show",
			"seats":   res.Seats,
		})
	}
}

func outcomeName(o store.Outcome) string {
	switch o {
	case store.OutcomeConfirmed:
		return "confirmed"
	case store.OutcomeReplayed:
		return metrics.ReasonIdempotentReplay
	case store.OutcomeIdempotencyConflict:
		return metrics.ReasonIdempotencyConflict
	case store.OutcomeSeatTaken:
		return metrics.ReasonSeatTaken
	case store.OutcomeLimitExceeded:
		return metrics.ReasonPerUserLimit
	case store.OutcomeUnknownSeats:
		return metrics.ReasonUnknownSeat
	}
	return "unknown"
}

// ---------------------------------------------------------------- reservations

func (s *Server) getReservation(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	ctx, cancel := dbCtx(r)
	defer cancel()
	res, err := s.st.GetReservation(ctx, r.PathValue("id"), uid)
	if s.reservationErr(w, r, err) {
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	ri := info(r)
	uid, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	ctx, cancel := dbCtx(r)
	defer cancel()
	res, changed, err := s.st.Cancel(ctx, r.PathValue("id"), uid)
	if s.reservationErr(w, r, err) {
		return
	}
	if changed {
		s.m.ReservationsCancelled.WithLabelValues(res.ShowID).Inc()
		s.m.SeatsReleased.WithLabelValues(res.ShowID).Add(float64(len(res.Seats)))
		ri.outcome = "cancelled"
		ri.log.Info("reservation cancelled", "show_id", res.ShowID, "reservation_id", res.ID, "seats", res.Seats)
	} else {
		ri.outcome = "already_cancelled"
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) reservationErr(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrReservationNotFound):
		writeJSON(w, http.StatusNotFound, errBody("reservation_not_found", "no such reservation"))
	case errors.Is(err, store.ErrNotOwner):
		info(r).outcome = "forbidden_not_owner"
		writeJSON(w, http.StatusForbidden, errBody("forbidden", "only the reservation owner may do this"))
	default:
		s.internal(w, r, err)
	}
	return true
}

// ---------------------------------------------------------------- helpers

// internal reports a genuine dependency failure. This is the only path that
// produces a 5xx, and it is counted and logged at error level.
func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	info(r).log.Error("internal error", "err", err)
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, errBody("unavailable", "temporarily unable to process request; safe to retry with the same idempotency key"))
}

func decode(w http.ResponseWriter, r *http.Request, v any, limit int64) bool {
	body := http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(body)
	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errBody("body_too_large", "request body too large"))
			return false
		}
		msg := "malformed JSON body"
		if errors.Is(err, io.EOF) {
			msg = "request body is required"
		}
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			msg = "field " + typeErr.Field + " has the wrong type (money is integer paise)"
		}
		writeJSON(w, http.StatusBadRequest, errBody("invalid_json", msg))
		return false
	}
	return true
}

func validLabel(l string) bool {
	if l == "" || len(l) > maxLabelLen {
		return false
	}
	for _, c := range l {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

func errBody(code, msg string) map[string]string {
	return map[string]string{"error": code, "message": msg}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
