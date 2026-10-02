// Package metrics exposes Prometheus metrics. Counters are incremented by the
// API after each domain outcome; seat gauges are read straight from the
// database at scrape time so they always reconcile with GET /shows/{id}.
package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/store"
)

// Decline reasons.
const (
	ReasonSeatTaken           = "seat_taken"
	ReasonPerUserLimit        = "per_user_limit"
	ReasonIdempotentReplay    = "idempotent_replay"
	ReasonIdempotencyConflict = "idempotency_conflict"
	ReasonUnknownSeat         = "unknown_seat"
)

type Metrics struct {
	Registry *prometheus.Registry

	ReservationsConfirmed *prometheus.CounterVec
	ReservationsDeclined  *prometheus.CounterVec
	SeatsConfirmed        *prometheus.CounterVec
	ReservationsCancelled *prometheus.CounterVec
	SeatsReleased         *prometheus.CounterVec
	FastPathDecisions     *prometheus.CounterVec
	HTTPRequests          *prometheus.CounterVec
	HTTPDuration          *prometheus.HistogramVec
	InFlight              prometheus.Gauge
	ReserveErrors         prometheus.Counter
}

func New(st *store.Store, pool *pgxpool.Pool, log *slog.Logger) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		ReservationsConfirmed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservations_confirmed_total",
			Help: "Reservations confirmed (201).",
		}, []string{"show_id"}),
		ReservationsDeclined: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservations_declined_total",
			Help: "Reserve requests that did not create a new reservation, by reason.",
		}, []string{"show_id", "reason"}),
		SeatsConfirmed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "seats_confirmed_total",
			Help: "Seats moved available -> confirmed.",
		}, []string{"show_id"}),
		ReservationsCancelled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reservations_cancelled_total",
			Help: "Reservations cancelled by their owner.",
		}, []string{"show_id"}),
		SeatsReleased: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "seats_released_total",
			Help: "Seats moved confirmed -> available by a cancel.",
		}, []string{"show_id"}),
		FastPathDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reserve_fast_path_total",
			Help: "Reserve outcomes decided by the lock-free pre-check, by outcome.",
		}, []string{"outcome"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by route and status code.",
		}, []string{"method", "route", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"method", "route"}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "HTTP requests currently being served.",
		}),
		ReserveErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reserve_internal_errors_total",
			Help: "Reserve attempts that failed with an infrastructure error (page on this).",
		}),
	}
	reg.MustRegister(
		m.ReservationsConfirmed, m.ReservationsDeclined, m.SeatsConfirmed,
		m.ReservationsCancelled, m.SeatsReleased, m.FastPathDecisions,
		m.HTTPRequests, m.HTTPDuration, m.InFlight, m.ReserveErrors,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		&seatCollector{st: st, log: log},
		&poolCollector{pool: pool},
	)
	return m
}

// ---------------------------------------------------------------- seat gauges

var (
	seatsDesc = prometheus.NewDesc("seats", "Seats per show by status (read from the database).",
		[]string{"show_id", "status"}, nil)
	seatsAvailableDesc = prometheus.NewDesc("seats_available", "Available seats per show (read from the database).",
		[]string{"show_id"}, nil)
	seatsTotalDesc = prometheus.NewDesc("seats_total", "Total seats per show.",
		[]string{"show_id"}, nil)
	invariantDesc = prometheus.NewDesc("seat_invariant_ok",
		"1 if available+held+confirmed == total_seats for the show, else 0.",
		[]string{"show_id"}, nil)
)

const showsTracked = 25

type seatCollector struct {
	st  *store.Store
	log *slog.Logger

	mu     sync.Mutex
	at     time.Time
	cached map[string]store.Counts
}

func (c *seatCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- seatsDesc
	ch <- seatsAvailableDesc
	ch <- seatsTotalDesc
	ch <- invariantDesc
}

func (c *seatCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	if time.Since(c.at) > 500*time.Millisecond {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		counts, err := c.st.SeatCountsByShow(ctx, showsTracked)
		cancel()
		if err != nil {
			c.log.Warn("metrics: seat counts query failed", "err", err)
		} else {
			c.cached, c.at = counts, time.Now()
		}
	}
	counts := c.cached
	c.mu.Unlock()

	for id, n := range counts {
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(n.Available), id, store.SeatAvailable)
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(n.Held), id, store.SeatHeld)
		ch <- prometheus.MustNewConstMetric(seatsDesc, prometheus.GaugeValue, float64(n.Confirmed), id, store.SeatConfirmed)
		ch <- prometheus.MustNewConstMetric(seatsAvailableDesc, prometheus.GaugeValue, float64(n.Available), id)
		ch <- prometheus.MustNewConstMetric(seatsTotalDesc, prometheus.GaugeValue, float64(n.Total), id)
		ok := 0.0
		if n.Available+n.Held+n.Confirmed == n.Total {
			ok = 1
		}
		ch <- prometheus.MustNewConstMetric(invariantDesc, prometheus.GaugeValue, ok, id)
	}
}

// ---------------------------------------------------------------- db pool

var (
	poolAcquiredDesc = prometheus.NewDesc("db_pool_acquired_conns", "Connections currently checked out.", nil, nil)
	poolIdleDesc     = prometheus.NewDesc("db_pool_idle_conns", "Idle connections.", nil, nil)
	poolMaxDesc      = prometheus.NewDesc("db_pool_max_conns", "Pool size limit.", nil, nil)
	poolWaitDesc     = prometheus.NewDesc("db_pool_empty_acquire_total", "Acquires that had to wait for a connection.", nil, nil)
	poolWaitDurDesc  = prometheus.NewDesc("db_pool_acquire_wait_seconds_total", "Total time spent waiting for a connection.", nil, nil)
)

type poolCollector struct{ pool *pgxpool.Pool }

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- poolAcquiredDesc
	ch <- poolIdleDesc
	ch <- poolMaxDesc
	ch <- poolWaitDesc
	ch <- poolWaitDurDesc
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(poolAcquiredDesc, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(poolIdleDesc, prometheus.GaugeValue, float64(s.IdleConns()))
	ch <- prometheus.MustNewConstMetric(poolMaxDesc, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(poolWaitDesc, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(poolWaitDurDesc, prometheus.CounterValue, s.EmptyAcquireWaitTime().Seconds())
}
