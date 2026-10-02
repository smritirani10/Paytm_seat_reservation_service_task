package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/api"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/auth"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/metrics"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(log)

	dbURL := env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/seats?sslmode=disable")
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Error("bad DATABASE_URL", "err", err)
		os.Exit(1)
	}
	cfg.MaxConns = int32(envInt("DB_MAX_CONNS", 40))
	cfg.MinConns = 2
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 15 * time.Second
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	// Safety net: no statement may wait on a lock forever.
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "20s"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "25s"
	cfg.ConnConfig.RuntimeParams["application_name"] = "seat-reservation"

	// pgxpool connects lazily, so this succeeds even if the DB is still
	// booting; readiness stays false until migrations have run.
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Error("pool init failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	jwtSecret := env("JWT_SECRET", "")
	if jwtSecret == "" {
		jwtSecret = "dev-insecure-secret-change-me"
		log.Warn("JWT_SECRET not set; using an insecure development secret")
	}
	adminToken := env("ADMIN_TOKEN", "")
	if adminToken == "" {
		adminToken = "dev-admin-token"
		log.Warn("ADMIN_TOKEN not set; using the development admin token")
	}

	st := store.New(pool)
	m := metrics.New(st, pool, log)
	ready := &atomic.Bool{}
	srv := api.New(st, auth.New(jwtSecret, adminToken, 24*time.Hour), m, log, ready)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go migrateUntilReady(ctx, st, ready, log)

	addr := ":" + env("PORT", "8080")
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Info("listening", "addr", addr, "db_max_conns", cfg.MaxConns)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	ready.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

// migrateUntilReady retries with backoff so the service survives a cold start
// where the database comes up after the app.
func migrateUntilReady(ctx context.Context, st *store.Store, ready *atomic.Bool, log *slog.Logger) {
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := st.Migrate(mctx)
		cancel()
		if err == nil {
			ready.Store(true)
			log.Info("database ready", "attempt", attempt)
			return
		}
		log.Warn("database not ready, retrying", "attempt", attempt, "err", err, "backoff", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil && n > 0 {
		return n
	}
	return def
}

func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return slog.LevelInfo
	}
	return l
}
