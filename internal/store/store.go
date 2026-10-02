// Package store is the system of record. Every correctness decision (who gets a
// seat, per-user limits, idempotency) is made inside a single Postgres
// transaction here; nothing above this layer is trusted to decide.
package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Seat statuses.
const (
	SeatAvailable = "available"
	SeatHeld      = "held"
	SeatConfirmed = "confirmed"
)

// Reservation statuses.
const (
	ResConfirmed = "confirmed"
	ResCancelled = "cancelled"
)

var (
	ErrShowNotFound        = errors.New("show not found")
	ErrReservationNotFound = errors.New("reservation not found")
	ErrNotOwner            = errors.New("reservation belongs to another user")
)

type Show struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	PricePaise   int64     `json:"price_paise"`
	PerUserLimit int       `json:"per_user_limit"`
	TotalSeats   int       `json:"total_seats"`
	CreatedAt    time.Time `json:"created_at"`
}

type SeatState struct {
	Label  string `json:"label"`
	Status string `json:"status"`
}

type Counts struct {
	Available int `json:"available"`
	Held      int `json:"held"`
	Confirmed int `json:"confirmed"`
	Total     int `json:"total_seats"`
}

type ShowState struct {
	Show
	Counts Counts      `json:"counts"`
	Seats  []SeatState `json:"seats"`
}

type Reservation struct {
	ID          string     `json:"reservation_id"`
	ShowID      string     `json:"show_id"`
	UserID      string     `json:"user_id"`
	Seats       []string   `json:"seats"`
	AmountPaise int64      `json:"amount_paise"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`

	fingerprint string
}

// Outcome is the domain result of a reserve attempt. Declines are outcomes,
// not errors: errors are reserved for genuine infrastructure failure.
type Outcome int

const (
	OutcomeConfirmed Outcome = iota
	OutcomeReplayed
	OutcomeSeatTaken
	OutcomeLimitExceeded
	OutcomeIdempotencyConflict
	OutcomeUnknownSeats
)

type ReserveResult struct {
	Outcome     Outcome
	Reservation *Reservation
	Seats       []string // taken or unknown seats, depending on outcome
	Held        int      // seats already held by the user (limit outcome)
	Limit       int
	FastPath    bool // decided without opening a locking transaction
}

type Store struct {
	pool *pgxpool.Pool

	mu    sync.RWMutex
	shows map[string]*Show // shows are immutable once created, safe to cache
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, shows: make(map[string]*Show)}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies the schema under a session advisory lock so several
// instances booting at once don't race each other.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(727274)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(727274)`)
	_, err = conn.Exec(ctx, schemaSQL)
	return err
}

func (s *Store) Ping(ctx context.Context) error {
	var one int
	return s.pool.QueryRow(ctx, `SELECT 1`).Scan(&one)
}

// ---------------------------------------------------------------- shows

func (s *Store) CreateShow(ctx context.Context, name string, labels []string, pricePaise int64, perUserLimit int) (*ShowState, error) {
	var show Show
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO shows (name, price_paise, per_user_limit, total_seats)
			VALUES ($1, $2, $3, $4)
			RETURNING id, name, price_paise, per_user_limit, total_seats, created_at`,
			name, pricePaise, perUserLimit, len(labels),
		).Scan(&show.ID, &show.Name, &show.PricePaise, &show.PerUserLimit, &show.TotalSeats, &show.CreatedAt)
		if err != nil {
			return err
		}
		ordinals := make([]int32, len(labels))
		for i := range ordinals {
			ordinals[i] = int32(i)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO seats (show_id, label, ordinal)
			SELECT $1, l, o FROM unnest($2::text[], $3::int[]) AS t(l, o)`,
			show.ID, labels, ordinals)
		return err
	})
	if err != nil {
		return nil, err
	}
	s.cacheShow(&show)

	seats := make([]SeatState, len(labels))
	for i, l := range labels {
		seats[i] = SeatState{Label: l, Status: SeatAvailable}
	}
	return &ShowState{
		Show:   show,
		Counts: Counts{Available: len(labels), Total: len(labels)},
		Seats:  seats,
	}, nil
}

func (s *Store) cacheShow(sh *Show) {
	s.mu.Lock()
	s.shows[sh.ID] = sh
	s.mu.Unlock()
}

func (s *Store) GetShowMeta(ctx context.Context, id string) (*Show, error) {
	if !IsUUID(id) {
		return nil, ErrShowNotFound
	}
	s.mu.RLock()
	sh, ok := s.shows[id]
	s.mu.RUnlock()
	if ok {
		return sh, nil
	}
	var show Show
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, price_paise, per_user_limit, total_seats, created_at
		FROM shows WHERE id = $1`, id,
	).Scan(&show.ID, &show.Name, &show.PricePaise, &show.PerUserLimit, &show.TotalSeats, &show.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrShowNotFound
	}
	if err != nil {
		return nil, err
	}
	s.cacheShow(&show)
	return &show, nil
}

// GetShowState reads every seat in one statement, so the counts are taken
// from a single MVCC snapshot and available+held+confirmed always equals
// total_seats exactly.
func (s *Store) GetShowState(ctx context.Context, id string) (*ShowState, error) {
	show, err := s.GetShowMeta(ctx, id)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT label, status FROM seats WHERE show_id = $1 ORDER BY ordinal`, id)
	if err != nil {
		return nil, err
	}
	seats, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (SeatState, error) {
		var st SeatState
		err := r.Scan(&st.Label, &st.Status)
		return st, err
	})
	if err != nil {
		return nil, err
	}
	c := Counts{Total: show.TotalSeats}
	for _, st := range seats {
		switch st.Status {
		case SeatAvailable:
			c.Available++
		case SeatHeld:
			c.Held++
		case SeatConfirmed:
			c.Confirmed++
		}
	}
	return &ShowState{Show: *show, Counts: c, Seats: seats}, nil
}

// ---------------------------------------------------------------- reserve

// Fingerprint identifies "the same request" for idempotency: same show and the
// same set of seats (order-insensitive).
func Fingerprint(showID string, seats []string) string {
	sorted := append([]string(nil), seats...)
	sort.Strings(sorted)
	return showID + "|" + strings.Join(sorted, ",")
}

const maxTxAttempts = 8

// Reserve is all-or-nothing: either every requested seat is confirmed to the
// caller in one reservation, or none are.
func (s *Store) Reserve(ctx context.Context, show *Show, userID string, seats []string, idemKey string) (*ReserveResult, error) {
	fp := Fingerprint(show.ID, seats)

	// Fast path: a single read-only statement (one snapshot) that answers
	// "is this a replay?" and "is any seat already taken?". Because a
	// reservation and its seat updates commit atomically, if this snapshot
	// shows a seat taken by our own earlier attempt it also shows that
	// attempt's reservation row, so a retry is never misreported as
	// seat_taken. A decline here is safe: the seat really was taken at the
	// snapshot instant. The authoritative decision is still the transaction.
	if res, err := s.fastPath(ctx, show, userID, seats, idemKey, fp); err != nil || res != nil {
		return res, err
	}

	var lastErr error
	for attempt := 0; attempt < maxTxAttempts; attempt++ {
		res, err := s.reserveTx(ctx, show, userID, seats, idemKey, fp)
		if err == nil {
			return res, nil
		}
		if !isRetryable(err) {
			return nil, err
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return nil, fmt.Errorf("reserve: retries exhausted: %w", lastErr)
}

func (s *Store) fastPath(ctx context.Context, show *Show, userID string, seats []string, idemKey, fp string) (*ReserveResult, error) {
	var (
		resID, resShow, resUser, resStatus, resFP *string
		resSeats                                  []string
		resAmount                                 *int64
		resCreated, resCancelled                  *time.Time
		found                                     int
		taken                                     []string
	)
	err := s.pool.QueryRow(ctx, `
		WITH r AS (
			SELECT id::text, show_id::text, user_id, seats, amount_paise, status,
			       request_fingerprint, created_at, cancelled_at
			FROM reservations WHERE user_id = $1 AND idempotency_key = $2
		), t AS (
			SELECT count(*)::int AS found,
			       coalesce(array_agg(label ORDER BY label) FILTER (WHERE status <> 'available'), '{}') AS taken
			FROM seats WHERE show_id = $3 AND label = ANY($4)
		)
		SELECT r.id, r.show_id, r.user_id, r.seats, r.amount_paise, r.status,
		       r.request_fingerprint, r.created_at, r.cancelled_at, t.found, t.taken
		FROM t LEFT JOIN r ON true`,
		userID, idemKey, show.ID, seats,
	).Scan(&resID, &resShow, &resUser, &resSeats, &resAmount, &resStatus, &resFP, &resCreated, &resCancelled, &found, &taken)
	if err != nil {
		return nil, err
	}
	if resID != nil {
		r := &Reservation{
			ID: *resID, ShowID: *resShow, UserID: *resUser, Seats: resSeats,
			AmountPaise: *resAmount, Status: *resStatus, CreatedAt: *resCreated,
			CancelledAt: resCancelled, fingerprint: *resFP,
		}
		return replayOrConflict(r, fp, true), nil
	}
	if found != len(seats) {
		return &ReserveResult{Outcome: OutcomeUnknownSeats, Seats: s.unknownSeats(ctx, show.ID, seats), FastPath: true}, nil
	}
	if len(taken) > 0 {
		return &ReserveResult{Outcome: OutcomeSeatTaken, Seats: taken, FastPath: true}, nil
	}
	return nil, nil
}

func replayOrConflict(r *Reservation, fp string, fast bool) *ReserveResult {
	if r.fingerprint == fp {
		return &ReserveResult{Outcome: OutcomeReplayed, Reservation: r, FastPath: fast}
	}
	return &ReserveResult{Outcome: OutcomeIdempotencyConflict, Reservation: r, FastPath: fast}
}

func (s *Store) unknownSeats(ctx context.Context, showID string, seats []string) []string {
	rows, err := s.pool.Query(ctx, `
		SELECT w FROM unnest($2::text[]) AS w
		WHERE NOT EXISTS (SELECT 1 FROM seats WHERE show_id = $1 AND label = w)
		ORDER BY w`, showID, seats)
	if err != nil {
		return nil
	}
	out, _ := pgx.CollectRows(rows, pgx.RowTo[string])
	return out
}

// reserveTx is the authoritative decision. Order of operations:
//
//  1. Per-user transaction-scoped advisory lock: serialises all reserve
//     attempts of one user (needed for the per-user limit and for concurrent
//     retries of the same idempotency key). Different users never contend.
//  2. Idempotency lookup under that lock.
//  3. Per-user limit check (safe: only this user's transactions can increase
//     this user's holdings, and they are serialised by step 1).
//  4. Row-lock the requested seats in a deterministic order (ORDER BY label
//     FOR UPDATE) so multi-seat requests can never deadlock each other.
//  5. Conditional update guarded on status = 'available'; the affected row
//     count must equal the number of seats requested.
//  6. Insert the reservation (carrying the idempotency key) and commit.
func (s *Store) reserveTx(ctx context.Context, show *Show, userID string, seats []string, idemKey, fp string) (*ReserveResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('user:' || $1, 0))`, userID); err != nil {
		return nil, err
	}

	if r, err := getReservationByKey(ctx, tx, userID, idemKey); err != nil {
		return nil, err
	} else if r != nil {
		return replayOrConflict(r, fp, false), nil
	}

	var held int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM seats
		WHERE show_id = $1 AND user_id = $2 AND status <> 'available'`,
		show.ID, userID).Scan(&held); err != nil {
		return nil, err
	}
	if held+len(seats) > show.PerUserLimit {
		return &ReserveResult{Outcome: OutcomeLimitExceeded, Held: held, Limit: show.PerUserLimit}, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT label, status FROM seats
		WHERE show_id = $1 AND label = ANY($2)
		ORDER BY label
		FOR UPDATE`, show.ID, seats)
	if err != nil {
		return nil, err
	}
	var taken []string
	locked := 0
	for rows.Next() {
		var label, status string
		if err := rows.Scan(&label, &status); err != nil {
			rows.Close()
			return nil, err
		}
		locked++
		if status != SeatAvailable {
			taken = append(taken, label)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if locked != len(seats) {
		return &ReserveResult{Outcome: OutcomeUnknownSeats, Seats: s.unknownSeats(ctx, show.ID, seats)}, nil
	}
	if len(taken) > 0 {
		return &ReserveResult{Outcome: OutcomeSeatTaken, Seats: taken}, nil
	}

	sorted := append([]string(nil), seats...)
	sort.Strings(sorted)
	r := &Reservation{
		ShowID:      show.ID,
		UserID:      userID,
		Seats:       sorted,
		AmountPaise: show.PricePaise * int64(len(seats)),
		Status:      ResConfirmed,
	}
	if err := tx.QueryRow(ctx, `
		INSERT INTO reservations (show_id, user_id, seats, amount_paise, status, idempotency_key, request_fingerprint)
		VALUES ($1, $2, $3, $4, 'confirmed', $5, $6)
		RETURNING id::text, created_at`,
		show.ID, userID, r.Seats, r.AmountPaise, idemKey, fp,
	).Scan(&r.ID, &r.CreatedAt); err != nil {
		return nil, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE seats
		SET status = 'confirmed', reservation_id = $3, user_id = $4, updated_at = now()
		WHERE show_id = $1 AND label = ANY($2) AND status = 'available'`,
		show.ID, seats, r.ID, userID)
	if err != nil {
		return nil, err
	}
	if int(tag.RowsAffected()) != len(seats) {
		// Unreachable while we hold the row locks; refuse rather than commit
		// a partial reservation.
		return &ReserveResult{Outcome: OutcomeSeatTaken, Seats: seats}, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &ReserveResult{Outcome: OutcomeConfirmed, Reservation: r}, nil
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func getReservationByKey(ctx context.Context, q querier, userID, key string) (*Reservation, error) {
	return scanReservation(q.QueryRow(ctx, `
		SELECT id::text, show_id::text, user_id, seats, amount_paise, status,
		       request_fingerprint, created_at, cancelled_at
		FROM reservations WHERE user_id = $1 AND idempotency_key = $2`, userID, key))
}

func scanReservation(row pgx.Row) (*Reservation, error) {
	var r Reservation
	err := row.Scan(&r.ID, &r.ShowID, &r.UserID, &r.Seats, &r.AmountPaise, &r.Status,
		&r.fingerprint, &r.CreatedAt, &r.CancelledAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ---------------------------------------------------------------- reservations

func (s *Store) GetReservation(ctx context.Context, id, userID string) (*Reservation, error) {
	if !IsUUID(id) {
		return nil, ErrReservationNotFound
	}
	r, err := scanReservation(s.pool.QueryRow(ctx, `
		SELECT id::text, show_id::text, user_id, seats, amount_paise, status,
		       request_fingerprint, created_at, cancelled_at
		FROM reservations WHERE id = $1`, id))
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, ErrReservationNotFound
	}
	if r.UserID != userID {
		return nil, ErrNotOwner
	}
	return r, nil
}

// Cancel releases a reservation's seats. Only the owner may cancel. The seat
// update is guarded on reservation_id, so a release can only ever touch seats
// that still point at *this* reservation and can never resurrect a seat that
// has since been sold to someone else. Cancelling twice is a no-op.
func (s *Store) Cancel(ctx context.Context, id, userID string) (r *Reservation, changed bool, err error) {
	if !IsUUID(id) {
		return nil, false, ErrReservationNotFound
	}
	for attempt := 0; attempt < maxTxAttempts; attempt++ {
		r, changed, err = s.cancelTx(ctx, id, userID)
		if err == nil || !isRetryable(err) {
			return r, changed, err
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return nil, false, err
}

func (s *Store) cancelTx(ctx context.Context, id, userID string) (*Reservation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(context.Background())

	r, err := scanReservation(tx.QueryRow(ctx, `
		SELECT id::text, show_id::text, user_id, seats, amount_paise, status,
		       request_fingerprint, created_at, cancelled_at
		FROM reservations WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		return nil, false, err
	}
	if r == nil {
		return nil, false, ErrReservationNotFound
	}
	if r.UserID != userID {
		return nil, false, ErrNotOwner
	}
	if r.Status == ResCancelled {
		return r, false, nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE seats
		SET status = 'available', reservation_id = NULL, user_id = NULL, updated_at = now()
		WHERE reservation_id = $1`, id); err != nil {
		return nil, false, err
	}
	if err := tx.QueryRow(ctx, `
		UPDATE reservations SET status = 'cancelled', cancelled_at = now()
		WHERE id = $1 RETURNING cancelled_at`, id).Scan(&r.CancelledAt); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	r.Status = ResCancelled
	return r, true, nil
}

// ---------------------------------------------------------------- audit

type Audit struct {
	ShowID                    string `json:"show_id"`
	OK                        bool   `json:"ok"`
	Counts                    Counts `json:"counts"`
	InvariantHolds            bool   `json:"invariant_holds"`
	ConfirmedReservations     int    `json:"confirmed_reservations"`
	CancelledReservations     int    `json:"cancelled_reservations"`
	SeatsInConfirmedRes       int    `json:"seats_in_confirmed_reservations"`
	OrphanSeats               int    `json:"orphan_seats"`
	BrokenReservations        int    `json:"broken_reservations"`
	MaxSeatsPerUser           int    `json:"max_seats_per_user"`
	PerUserLimit              int    `json:"per_user_limit"`
	UsersOverLimit            int    `json:"users_over_limit"`
	ConfirmedRevenuePaise     int64  `json:"confirmed_revenue_paise"`
	ExpectedRevenuePaise      int64  `json:"expected_revenue_paise"`
	DistinctUsersHoldingSeats int    `json:"distinct_users_holding_seats"`
}

// AuditShow cross-checks the seats table against the reservations table in a
// single REPEATABLE READ snapshot.
func (s *Store) AuditShow(ctx context.Context, id string) (*Audit, error) {
	show, err := s.GetShowMeta(ctx, id)
	if err != nil {
		return nil, err
	}
	a := &Audit{ShowID: id, PerUserLimit: show.PerUserLimit}
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status = 'available'),
			       count(*) FILTER (WHERE status = 'held'),
			       count(*) FILTER (WHERE status = 'confirmed'),
			       count(*)
			FROM seats WHERE show_id = $1`, id,
		).Scan(&a.Counts.Available, &a.Counts.Held, &a.Counts.Confirmed, &a.Counts.Total); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status = 'confirmed'),
			       count(*) FILTER (WHERE status = 'cancelled'),
			       coalesce(sum(cardinality(seats)) FILTER (WHERE status = 'confirmed'), 0),
			       coalesce(sum(amount_paise) FILTER (WHERE status = 'confirmed'), 0)
			FROM reservations WHERE show_id = $1`, id,
		).Scan(&a.ConfirmedReservations, &a.CancelledReservations, &a.SeatsInConfirmedRes, &a.ConfirmedRevenuePaise); err != nil {
			return err
		}
		// Seats marked taken whose reservation is missing, cancelled, belongs
		// to another user, or does not list the seat.
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM seats s
			LEFT JOIN reservations r ON r.id = s.reservation_id
			WHERE s.show_id = $1 AND s.status <> 'available'
			  AND (r.id IS NULL OR r.status <> 'confirmed' OR r.user_id <> s.user_id
			       OR NOT (s.label = ANY(r.seats)))`, id,
		).Scan(&a.OrphanSeats); err != nil {
			return err
		}
		// Confirmed reservations that don't own every seat they list.
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM reservations r
			WHERE r.show_id = $1 AND r.status = 'confirmed'
			  AND cardinality(r.seats) <> (SELECT count(*) FROM seats s WHERE s.reservation_id = r.id)`, id,
		).Scan(&a.BrokenReservations); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			SELECT coalesce(max(c), 0), count(*) FILTER (WHERE c > $2), count(*)
			FROM (SELECT count(*) AS c FROM seats
			      WHERE show_id = $1 AND status <> 'available' GROUP BY user_id) per_user`,
			id, show.PerUserLimit,
		).Scan(&a.MaxSeatsPerUser, &a.UsersOverLimit, &a.DistinctUsersHoldingSeats)
	})
	if err != nil {
		return nil, err
	}
	a.ExpectedRevenuePaise = int64(a.Counts.Confirmed) * show.PricePaise
	a.InvariantHolds = a.Counts.Available+a.Counts.Held+a.Counts.Confirmed == show.TotalSeats &&
		a.Counts.Total == show.TotalSeats
	a.OK = a.InvariantHolds &&
		a.OrphanSeats == 0 &&
		a.BrokenReservations == 0 &&
		a.UsersOverLimit == 0 &&
		a.SeatsInConfirmedRes == a.Counts.Confirmed+a.Counts.Held &&
		a.ConfirmedRevenuePaise == a.ExpectedRevenuePaise
	return a, nil
}

// SeatCountsByShow returns per-status seat counts for the most recent shows;
// used by the metrics collector so gauges come straight from the database.
func (s *Store) SeatCountsByShow(ctx context.Context, limit int) (map[string]Counts, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT s.id::text, s.total_seats,
		       count(*) FILTER (WHERE st.status = 'available'),
		       count(*) FILTER (WHERE st.status = 'held'),
		       count(*) FILTER (WHERE st.status = 'confirmed')
		FROM (SELECT id, total_seats FROM shows ORDER BY created_at DESC LIMIT $1) s
		JOIN seats st ON st.show_id = s.id
		GROUP BY s.id, s.total_seats`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Counts{}
	for rows.Next() {
		var id string
		var c Counts
		if err := rows.Scan(&id, &c.Total, &c.Available, &c.Held, &c.Confirmed); err != nil {
			return nil, err
		}
		out[id] = c
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- helpers

// isRetryable reports transient concurrency failures that are safe to retry
// from scratch: deadlock, serialization failure, lock timeout, and a unique
// violation on the idempotency key (the retry will then find the winner's row
// and replay it).
func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40P01", "40001", "55P03", "23505":
			return true
		}
	}
	return false
}

func IsUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}
