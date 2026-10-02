package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/api"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/auth"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/metrics"
	"github.com/smritirani10/paytm_seat_reservation_service_task/internal/store"
)

// Integration tests: they need a real Postgres (TEST_DATABASE_URL), because
// the whole point is exercising the database's locking under concurrency.

const adminToken = "test-admin"

var (
	baseURL string
	authn   = auth.New("test-secret", adminToken, time.Hour)
	client  = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1000, MaxConnsPerHost: 0}}
)

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		fmt.Println("TEST_DATABASE_URL not set; skipping integration tests")
		os.Exit(0)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		panic(err)
	}
	cfg.MaxConns = 30
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		panic(err)
	}
	st := store.New(pool)
	if err := st.Migrate(context.Background()); err != nil {
		panic(err)
	}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ready := &atomic.Bool{}
	ready.Store(true)
	srv := httptest.NewServer(api.New(st, authn, metrics.New(st, pool, log), log, ready).Handler())
	baseURL = srv.URL
	code := m.Run()
	srv.Close()
	pool.Close()
	os.Exit(code)
}

type resp struct {
	Code int
	Body map[string]any
}

func do(t *testing.T, method, path, token string, body any) resp {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, baseURL+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Errorf("request failed: %v", err)
		return resp{}
	}
	defer res.Body.Close()
	out := resp{Code: res.StatusCode}
	_ = json.NewDecoder(res.Body).Decode(&out.Body)
	return out
}

var runID atomic.Int64

// token issues a token for a user name unique to this test run, so reruns
// (go test -count=N) never collide on per-user idempotency keys.
func token(t *testing.T, user string) string {
	tok, _, err := authn.Issue(fmt.Sprintf("%s-r%d", user, runID.Load()))
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func newShow(t *testing.T, n, limit int) string {
	t.Helper()
	runID.Store(time.Now().UnixNano())
	seats := make([]string, n)
	for i := range seats {
		seats[i] = fmt.Sprintf("S%d", i+1)
	}
	r := do(t, "POST", "/shows", adminToken, map[string]any{
		"name": t.Name(), "seats": seats, "price_paise": 25000, "per_user_limit": limit,
	})
	if r.Code != 201 {
		t.Fatalf("create show: %d %v", r.Code, r.Body)
	}
	return r.Body["id"].(string)
}

func reserve(t *testing.T, show, tok, key string, seats ...string) resp {
	return do(t, "POST", "/shows/"+show+"/reserve", tok, map[string]any{"seats": seats, "idempotency_key": key})
}

func assertAudit(t *testing.T, show string) map[string]any {
	t.Helper()
	r := do(t, "GET", "/shows/"+show+"/audit", "", nil)
	if r.Code != 200 || r.Body["ok"] != true {
		t.Fatalf("audit failed: %d %v", r.Code, r.Body)
	}
	return r.Body
}

func parallel(n int, f func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			f(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

func TestHotSeatExactlyOneWinner(t *testing.T) {
	show := newShow(t, 10, 4)
	var mu sync.Mutex
	codes := map[int]int{}
	parallel(500, func(i int) {
		r := reserve(t, show, token(t, fmt.Sprintf("hot-%d", i)), "k", "S1")
		mu.Lock()
		codes[r.Code]++
		mu.Unlock()
	})
	if codes[201] != 1 || codes[409] != 499 {
		t.Fatalf("want exactly one 201 and 499 409, got %v", codes)
	}
	a := assertAudit(t, show)
	if a["counts"].(map[string]any)["confirmed"].(float64) != 1 {
		t.Fatalf("want 1 confirmed seat: %v", a)
	}
}

func TestPerUserLimitUnderConcurrency(t *testing.T) {
	show := newShow(t, 20, 4)
	tok := token(t, "greedy")
	var ok atomic.Int32
	parallel(10, func(i int) {
		r := reserve(t, show, tok, fmt.Sprintf("k%d", i), fmt.Sprintf("S%d", i+1))
		switch r.Code {
		case 201:
			ok.Add(1)
		case 409:
			if r.Body["error"] != "per_user_limit_exceeded" {
				t.Errorf("unexpected decline: %v", r.Body)
			}
		default:
			t.Errorf("unexpected status %d %v", r.Code, r.Body)
		}
	})
	if ok.Load() != 4 {
		t.Fatalf("want exactly 4 confirmed, got %d", ok.Load())
	}
	if a := assertAudit(t, show); a["max_seats_per_user"].(float64) != 4 {
		t.Fatalf("audit: %v", a)
	}
	// A multi-seat request that would cross the limit is declined whole.
	show2 := newShow(t, 20, 4)
	if r := reserve(t, show2, tok, "big", "S1", "S2", "S3", "S4", "S5"); r.Code != 409 {
		t.Fatalf("want 409 for 5 seats, got %d", r.Code)
	}
}

func TestIdempotentRetriesReserveOnce(t *testing.T) {
	show := newShow(t, 10, 4)
	tok := token(t, "retrier")
	var mu sync.Mutex
	codes := map[int]int{}
	ids := map[string]bool{}
	parallel(50, func(i int) {
		r := reserve(t, show, tok, "same-key", "S3", "S4")
		mu.Lock()
		defer mu.Unlock()
		codes[r.Code]++
		if id, ok := r.Body["reservation_id"].(string); ok {
			ids[id] = true
		}
	})
	if codes[201] != 1 || codes[200] != 49 || len(ids) != 1 {
		t.Fatalf("want one 201 + 49 replays of one reservation, got codes=%v ids=%d", codes, len(ids))
	}
	// Order of seats doesn't change the request identity.
	if r := reserve(t, show, tok, "same-key", "S4", "S3"); r.Code != 200 {
		t.Fatalf("reordered replay: %d", r.Code)
	}
	// Different body, same key -> 409, and nothing moves.
	if r := reserve(t, show, tok, "same-key", "S5"); r.Code != 409 || r.Body["error"] != "idempotency_key_conflict" {
		t.Fatalf("want idempotency conflict, got %d %v", r.Code, r.Body)
	}
	a := assertAudit(t, show)
	if a["counts"].(map[string]any)["confirmed"].(float64) != 2 {
		t.Fatalf("want 2 confirmed seats: %v", a)
	}
}

func TestSameKeyDifferentBodiesRacing(t *testing.T) {
	show := newShow(t, 10, 4)
	tok := token(t, "racer")
	var mu sync.Mutex
	codes := map[int]int{}
	parallel(20, func(i int) {
		r := reserve(t, show, tok, "k", fmt.Sprintf("S%d", i%2+1))
		mu.Lock()
		codes[r.Code]++
		mu.Unlock()
	})
	if codes[201] != 1 || codes[200]+codes[409] != 19 {
		t.Fatalf("codes %v", codes)
	}
	a := assertAudit(t, show)
	if a["counts"].(map[string]any)["confirmed"].(float64) != 1 {
		t.Fatalf("audit %v", a)
	}
}

func TestMultiSeatNoDeadlockAllOrNothing(t *testing.T) {
	show := newShow(t, 30, 4)
	var fivexx atomic.Int32
	// Overlapping pairs requested in opposite orders: the classic deadlock shape.
	parallel(400, func(i int) {
		a, b := fmt.Sprintf("S%d", i%10+1), fmt.Sprintf("S%d", (i+1)%10+1)
		if i%2 == 0 {
			a, b = b, a
		}
		r := reserve(t, show, token(t, fmt.Sprintf("pair-%d", i)), "k", a, b)
		if r.Code >= 500 || r.Code == 0 {
			fivexx.Add(1)
		}
	})
	if fivexx.Load() != 0 {
		t.Fatalf("%d server errors", fivexx.Load())
	}
	audit := assertAudit(t, show)
	// All-or-nothing: confirmed seats are always an even number here.
	if int(audit["counts"].(map[string]any)["confirmed"].(float64))%2 != 0 {
		t.Fatalf("partial reservation detected: %v", audit)
	}
}

func TestCancelOwnershipAndRebook(t *testing.T) {
	show := newShow(t, 5, 4)
	alice, bob := token(t, "alice"), token(t, "bob")

	// Spoofed body field is ignored; identity is the token's.
	r := do(t, "POST", "/shows/"+show+"/reserve", alice, map[string]any{"seats": []string{"S1"}, "idempotency_key": "a1", "user_id": "bob"})
	if r.Code != 201 || r.Body["user_id"] == "bob" || r.Body["user_id"] != fmt.Sprintf("alice-r%d", runID.Load()) {
		t.Fatalf("reserve: %d %v", r.Code, r.Body)
	}
	rid := r.Body["reservation_id"].(string)

	if r := do(t, "POST", "/reservations/"+rid+"/cancel", bob, nil); r.Code != 403 {
		t.Fatalf("non-owner cancel: want 403 got %d", r.Code)
	}
	if r := do(t, "POST", "/reservations/"+rid+"/cancel", "", nil); r.Code != 401 {
		t.Fatalf("anonymous cancel: want 401 got %d", r.Code)
	}
	if r := do(t, "POST", "/reservations/"+rid+"/cancel", alice, nil); r.Code != 200 || r.Body["status"] != "cancelled" {
		t.Fatalf("owner cancel: %d %v", r.Code, r.Body)
	}
	// Seat is cleanly re-bookable.
	r = reserve(t, show, bob, "b1", "S1")
	if r.Code != 201 {
		t.Fatalf("rebook: %d %v", r.Code, r.Body)
	}
	// A repeated cancel of alice's old reservation must not free bob's seat.
	if r := do(t, "POST", "/reservations/"+rid+"/cancel", alice, nil); r.Code != 200 {
		t.Fatalf("second cancel: %d", r.Code)
	}
	st := do(t, "GET", "/shows/"+show, "", nil)
	if st.Body["counts"].(map[string]any)["confirmed"].(float64) != 1 {
		t.Fatalf("cancel resurrected a seat: %v", st.Body["counts"])
	}
	// Replaying alice's original key returns the original (cancelled) reservation, not a new one.
	if r := reserve(t, show, alice, "a1", "S1"); r.Code != 200 || r.Body["status"] != "cancelled" {
		t.Fatalf("replay after cancel: %d %v", r.Code, r.Body)
	}
	assertAudit(t, show)
}

func TestCancelRebookStorm(t *testing.T) {
	show := newShow(t, 3, 4)
	var fivexx atomic.Int32
	parallel(60, func(i int) {
		tok := token(t, fmt.Sprintf("churn-%d", i))
		for j := 0; j < 5; j++ {
			r := reserve(t, show, tok, fmt.Sprintf("k%d", j), "S1")
			if r.Code >= 500 {
				fivexx.Add(1)
			}
			if r.Code == 201 {
				c := do(t, "POST", "/reservations/"+r.Body["reservation_id"].(string)+"/cancel", tok, nil)
				if c.Code != 200 {
					fivexx.Add(1)
				}
			}
		}
	})
	if fivexx.Load() != 0 {
		t.Fatalf("%d errors", fivexx.Load())
	}
	assertAudit(t, show)
}

func TestValidation(t *testing.T) {
	show := newShow(t, 5, 4)
	tok := token(t, "v")
	cases := []struct {
		name string
		body string
		want int
	}{
		{"float price", `{"name":"x","seats":["A"],"price_paise":250.5}`, 400},
	}
	for _, c := range cases {
		req, _ := http.NewRequest("POST", baseURL+"/shows", bytes.NewBufferString(c.body))
		req.Header.Set("Authorization", "Bearer "+adminToken)
		res, _ := client.Do(req)
		res.Body.Close()
		if res.StatusCode != c.want {
			t.Errorf("%s: got %d", c.name, res.StatusCode)
		}
	}
	if r := do(t, "POST", "/shows", tok, map[string]any{"name": "x", "seats": []string{"A"}, "price_paise": 1}); r.Code != 401 {
		t.Errorf("non-admin create: %d", r.Code)
	}
	if r := reserve(t, show, tok, "dup", "S1", "S1"); r.Code != 400 {
		t.Errorf("duplicate seats: %d", r.Code)
	}
	if r := reserve(t, show, tok, "nope", "Z9"); r.Code != 400 {
		t.Errorf("unknown seat: %d", r.Code)
	}
	if r := reserve(t, "00000000-0000-0000-0000-000000000000", tok, "x", "S1"); r.Code != 404 {
		t.Errorf("unknown show: %d", r.Code)
	}
	if r := reserve(t, "not-a-uuid", tok, "x", "S1"); r.Code != 404 {
		t.Errorf("bad show id: %d", r.Code)
	}
	if r := do(t, "POST", "/shows/"+show+"/reserve", "garbage", map[string]any{"seats": []string{"S1"}}); r.Code != 401 {
		t.Errorf("bad token: %d", r.Code)
	}
}
