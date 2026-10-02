// Command burst reproduces the on-sale stampede against a running deployment
// and verifies the correctness bar from the outside:
//
//   - hot-seat storm: many users, same seat -> exactly one 201, rest 409
//   - zero 5xx across the whole burst
//   - available+held+confirmed == total_seats during and after the burst
//   - idempotent retries create one reservation; same key + other seats -> 409
//   - a user firing 10 parallel reserves on a limit=4 show ends with <= 4
//   - a spoofed body user_id never changes who the seat is booked for
//   - metrics reconcile with what the client observed and with GET /shows/{id}
//
// Usage: go run ./cmd/burst -url https://host [-requests 20000]
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type kind string

const (
	kHot      kind = "hot-seat-storm"
	kRetry    kind = "idempotent-retry"
	kConflict kind = "same-key-diff-body"
	kLimit    kind = "per-user-limit"
	kSpoof    kind = "spoofed-user"
	kCrowd    kind = "general-crowd"
)

type plannedReq struct {
	kind  kind
	group int // hot seat index / retry group / user index
	user  string
	key   string
	seats []string
	body  []byte

	// results
	code     int
	resID    string
	resUser  string
	errCode  string
	resSeats []string
	latency  time.Duration
	netErr   string
}

var (
	baseURL    string
	adminToken string
	httpc      *http.Client
)

func main() {
	var (
		requests    = flag.Int("requests", 20000, "total reserve requests in the burst")
		concurrency = flag.Int("concurrency", 20000, "max requests in flight at once")
		nSeats      = flag.Int("seats", 2500, "seats in the fresh show")
		hot         = flag.Int("hot", 5, "number of hot seats to storm")
		storm       = flag.Int("storm", 500, "distinct users storming each hot seat")
		retryGroups = flag.Int("retry-groups", 1000, "users that send the same request 3x with the same key")
		conflicts   = flag.Int("conflict-groups", 200, "users that reuse a key with different seats")
		limitUsers  = flag.Int("limit-users", 100, "users firing 10 parallel single-seat reserves")
		spoofs      = flag.Int("spoofs", 100, "requests carrying a spoofed user_id body field")
		limit       = flag.Int("limit", 4, "per_user_limit for the show")
		timeout     = flag.Duration("timeout", 90*time.Second, "per-request client timeout")
		jsonOut     = flag.String("json", "", "optional path to write a JSON report")
	)
	flag.StringVar(&baseURL, "url", os.Getenv("BASE_URL"), "base URL of the service")
	flag.StringVar(&adminToken, "admin-token", envOr("ADMIN_TOKEN", "dev-admin-token"), "admin token for POST /shows")
	flag.Parse()
	if baseURL == "" && flag.NArg() > 0 {
		baseURL = flag.Arg(0)
	}
	if baseURL == "" {
		fmt.Fprintln(os.Stderr, "usage: burst -url <BASE_URL> (or BASE_URL env)")
		os.Exit(2)
	}
	baseURL = strings.TrimRight(baseURL, "/")
	httpc = &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency,
			MaxIdleConnsPerHost: *concurrency,
			MaxConnsPerHost:     *concurrency,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	section("1. setup")
	waitReady()
	labels := seatLabels(*nSeats)
	showID := createShow(labels, *limit)
	fmt.Printf("show %s: %d seats, per_user_limit=%d, price 25000 paise\n", showID, *nSeats, *limit)

	// Hot seats come from the middle of row A; everything else is "cold".
	hotSeats := pickHot(labels, *hot)
	isHot := map[string]bool{}
	for _, h := range hotSeats {
		isHot[h] = true
	}
	var cold []string
	for _, l := range labels {
		if !isHot[l] {
			cold = append(cold, l)
		}
	}

	// Limit-test users get a private block of 10 seats each, so the only thing
	// that can stop them is the per-user limit (not a sold-out show).
	if len(cold) < *limitUsers*10+100 {
		fatal("need at least %d seats for this plan", *hot+*limitUsers*10+100)
	}
	limitBlock, common := cold[:*limitUsers*10], cold[*limitUsers*10:]
	plan := buildPlan(*requests, hotSeats, limitBlock, common, *storm, *retryGroups, *conflicts, *limitUsers, *spoofs)
	users := map[string]bool{}
	for _, p := range plan {
		users[p.user] = true
	}
	fmt.Printf("planned %d reserve requests from %d distinct users; hot seats %v\n", len(plan), len(users), hotSeats)
	tokens := fetchTokens(users)

	for _, p := range plan {
		body := map[string]any{"seats": p.seats, "idempotency_key": p.key}
		if p.kind == kSpoof {
			body["user_id"] = "victim-user"
		}
		p.body, _ = json.Marshal(body)
	}

	section("2. burst")
	stopPoll := make(chan struct{})
	pollDone := make(chan pollStats)
	go pollInvariant(showID, *nSeats, stopPoll, pollDone)

	start := make(chan struct{})
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	var done atomic.Int64
	for _, p := range plan {
		wg.Add(1)
		go func(p *plannedReq) {
			defer wg.Done()
			<-start
			sem <- struct{}{}
			fire(p, showID, tokens[p.user])
			<-sem
			done.Add(1)
		}(p)
	}
	t0 := time.Now()
	close(start)
	progressDone := make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-tk.C:
				fmt.Printf("  ... %d/%d done (%.0fs)\n", done.Load(), len(plan), time.Since(t0).Seconds())
			}
		}
	}()
	wg.Wait()
	close(progressDone)
	elapsed := time.Since(t0)
	close(stopPoll)
	polls := <-pollDone
	fmt.Printf("burst finished: %d requests in %s (%.0f req/s)\n", len(plan), elapsed.Round(time.Millisecond), float64(len(plan))/elapsed.Seconds())

	section("3. outcome distribution")
	rep := analyse(plan, hotSeats, *limit)
	rep.print()

	section("4. release check")
	releaseOK := releaseCheck(showID, plan, hotSeats[0], tokens)

	section("5. final reconciliation")
	final := getShow(showID)
	audit := getJSON("/shows/" + showID + "/audit")
	met := scrapeMetrics(showID)

	checks := []check{}
	add := func(name string, ok bool, detail string) { checks = append(checks, check{name, ok, detail}) }

	for _, h := range hotSeats {
		w := rep.hotWins[h]
		add("hot seat "+h+": exactly one 201", w == 1, fmt.Sprintf("201=%d 409=%d other=%d", w, rep.hotDeclines[h], rep.hotOther[h]))
	}
	add("zero 5xx across the burst", rep.fivexx == 0, fmt.Sprintf("5xx=%d network_errors=%d", rep.fivexx, rep.netErrs))
	add("invariant held during burst", polls.violations == 0 && polls.errors5xx == 0,
		fmt.Sprintf("%d polls, %d violations, %d poll 5xx", polls.polls, polls.violations, polls.errors5xx))
	c := final.Counts
	add("invariant holds after burst", c.Available+c.Held+c.Confirmed == *nSeats,
		fmt.Sprintf("available=%d held=%d confirmed=%d total=%d", c.Available, c.Held, c.Confirmed, *nSeats))
	add("idempotent retries: one reservation per key", rep.retryViolations == 0,
		fmt.Sprintf("%d groups, %d violations", *retryGroups, rep.retryViolations))
	add("same key + different seats -> 409", rep.conflictViolations == 0 && rep.conflict409 > 0,
		fmt.Sprintf("%d idempotency_key_conflict, %d violations", rep.conflict409, rep.conflictViolations))
	add("per-user limit: 10 parallel reserves -> exactly limit confirmed", rep.limitViolations == 0,
		fmt.Sprintf("%d users; 201s per user min=%d max=%d (limit %d)", *limitUsers, rep.minPerLimitUser, rep.maxPerLimitUser, *limit))
	add("spoofed user_id ignored (token identity wins)", rep.spoofViolations == 0,
		fmt.Sprintf("%d spoofed requests, %d violations", *spoofs, rep.spoofViolations))
	add("server-side audit ok", audit["ok"] == true, fmt.Sprintf("max_seats_per_user=%v orphan_seats=%v broken_reservations=%v",
		audit["max_seats_per_user"], audit["orphan_seats"], audit["broken_reservations"]))
	add("release: cancelled seat is re-bookable, non-owner cannot cancel", releaseOK, "")

	expectConfirmedSeats := rep.confirmedSeats + releaseDelta
	add("confirmed seats == seats in 201 responses", c.Confirmed == expectConfirmedSeats,
		fmt.Sprintf("api=%d client_observed=%d", c.Confirmed, expectConfirmedSeats))
	if met.ok {
		add("metric seats_available == API available", int(met.available) == c.Available,
			fmt.Sprintf("metric=%v api=%d", met.available, c.Available))
		add("metric reservations_confirmed_total == observed 201s", int(met.confirmed) == rep.codes[201]+releaseConfirms,
			fmt.Sprintf("metric=%v observed=%d", met.confirmed, rep.codes[201]+releaseConfirms))
		add("metric declined{seat_taken} == observed", int(met.declined["seat_taken"]) == rep.reasons["seat_taken"],
			fmt.Sprintf("metric=%v observed=%d", met.declined["seat_taken"], rep.reasons["seat_taken"]))
		add("metric declined{per_user_limit} == observed", int(met.declined["per_user_limit"]) == rep.reasons["per_user_limit_exceeded"],
			fmt.Sprintf("metric=%v observed=%d", met.declined["per_user_limit"], rep.reasons["per_user_limit_exceeded"]))
		add("metric declined{idempotent_replay} == observed", int(met.declined["idempotent_replay"]) == rep.codes[200],
			fmt.Sprintf("metric=%v observed=%d", met.declined["idempotent_replay"], rep.codes[200]))
	} else {
		add("metrics scraped", false, "could not read /metrics")
	}

	failed := 0
	for _, ch := range checks {
		mark := "PASS"
		if !ch.OK {
			mark = "FAIL"
			failed++
		}
		fmt.Printf("  [%s] %-55s %s\n", mark, ch.Name, ch.Detail)
	}
	if rep.netErrs > 0 {
		fmt.Printf("\n  note: %d client-side network errors/timeouts (no HTTP status received); counter reconciliation may be off by those.\n", rep.netErrs)
	}

	if *jsonOut != "" {
		writeReport(*jsonOut, showID, elapsed, rep, final, audit, checks)
	}
	section("RESULT")
	if failed > 0 {
		fmt.Printf("%d of %d checks FAILED\n", failed, len(checks))
		os.Exit(1)
	}
	fmt.Printf("all %d checks passed\n", len(checks))
}

// ---------------------------------------------------------------- plan

func buildPlan(total int, hot, limitBlock, cold []string, storm, retryGroups, conflicts, limitUsers, spoofs int) []*plannedReq {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	run := strconv.FormatInt(time.Now().Unix()%1_000_000, 36)
	var plan []*plannedReq
	add := func(p *plannedReq) { plan = append(plan, p) }

	for h, seat := range hot {
		for i := 0; i < storm; i++ {
			add(&plannedReq{kind: kHot, group: h, user: fmt.Sprintf("hot-%s-%d-%d", run, h, i), key: "k-" + seat, seats: []string{seat}})
		}
	}
	for g := 0; g < retryGroups; g++ {
		u := fmt.Sprintf("retry-%s-%d", run, g)
		seats := []string{cold[rng.Intn(len(cold))]}
		for c := 0; c < 3; c++ {
			add(&plannedReq{kind: kRetry, group: g, user: u, key: "retry-key", seats: seats})
		}
	}
	for g := 0; g < conflicts; g++ {
		u := fmt.Sprintf("conflict-%s-%d", run, g)
		a, b := cold[rng.Intn(len(cold))], cold[rng.Intn(len(cold))]
		for b == a {
			b = cold[rng.Intn(len(cold))]
		}
		add(&plannedReq{kind: kConflict, group: g, user: u, key: "conflict-key", seats: []string{a}})
		add(&plannedReq{kind: kConflict, group: g, user: u, key: "conflict-key", seats: []string{b}})
	}
	for g := 0; g < limitUsers; g++ {
		u := fmt.Sprintf("limit-%s-%d", run, g)
		for i := 0; i < 10; i++ {
			add(&plannedReq{kind: kLimit, group: g, user: u, key: fmt.Sprintf("limit-%d", i), seats: []string{limitBlock[g*10+i]}})
		}
	}
	for i := 0; i < spoofs; i++ {
		add(&plannedReq{kind: kSpoof, group: i, user: fmt.Sprintf("spoof-%s-%d", run, i), key: "spoof", seats: []string{cold[rng.Intn(len(cold))]}})
	}
	crowdUsers := 4000
	for i := 0; len(plan) < total; i++ {
		n := 1 + rng.Intn(3)
		seen := map[string]bool{}
		var seats []string
		for len(seats) < n {
			s := cold[rng.Intn(len(cold))]
			if !seen[s] {
				seen[s] = true
				seats = append(seats, s)
			}
		}
		add(&plannedReq{kind: kCrowd, group: i, user: fmt.Sprintf("crowd-%s-%d", run, rng.Intn(crowdUsers)), key: fmt.Sprintf("crowd-%d", i), seats: seats})
	}
	rng.Shuffle(len(plan), func(i, j int) { plan[i], plan[j] = plan[j], plan[i] })
	return plan
}

func seatLabels(n int) []string {
	const perRow = 25
	out := make([]string, n)
	for i := range out {
		row := i / perRow
		name := ""
		for r := row; ; r = r/26 - 1 {
			name = string(rune('A'+r%26)) + name
			if r < 26 {
				break
			}
		}
		out[i] = fmt.Sprintf("%s%d", name, i%perRow+1)
	}
	return out
}

func pickHot(labels []string, n int) []string {
	start := 11 // A12, A13, ... the "good" seats
	if start+n > len(labels) {
		start = 0
	}
	if n > len(labels) {
		n = len(labels)
	}
	return append([]string(nil), labels[start:start+n]...)
}

// ---------------------------------------------------------------- http

func fire(p *plannedReq, showID, token string) {
	req, _ := http.NewRequest("POST", baseURL+"/shows/"+showID+"/reserve", bytes.NewReader(p.body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	t := time.Now()
	res, err := httpc.Do(req)
	p.latency = time.Since(t)
	if err != nil {
		p.netErr = err.Error()
		return
	}
	defer res.Body.Close()
	p.code = res.StatusCode
	var body struct {
		ReservationID string   `json:"reservation_id"`
		UserID        string   `json:"user_id"`
		Seats         []string `json:"seats"`
		Error         string   `json:"error"`
	}
	_ = json.NewDecoder(res.Body).Decode(&body)
	p.resID, p.resUser, p.resSeats, p.errCode = body.ReservationID, body.UserID, body.Seats, body.Error
}

func waitReady() {
	deadline := time.Now().Add(3 * time.Minute)
	for {
		res, err := httpc.Get(baseURL + "/readyz")
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				fmt.Println("service is ready")
				return
			}
		}
		if time.Now().After(deadline) {
			fatal("service never became ready at %s/readyz", baseURL)
		}
		fmt.Println("waiting for /readyz (cold start?) ...")
		time.Sleep(3 * time.Second)
	}
}

func createShow(labels []string, limit int) string {
	b, _ := json.Marshal(map[string]any{
		"name": "burst-" + time.Now().UTC().Format("20060102-150405"), "seats": labels,
		"price_paise": 25000, "per_user_limit": limit,
	})
	req, _ := http.NewRequest("POST", baseURL+"/shows", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	res, err := httpc.Do(req)
	if err != nil {
		fatal("create show: %v", err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	if res.StatusCode != 201 {
		fatal("create show: HTTP %d %v (is ADMIN_TOKEN right?)", res.StatusCode, out)
	}
	return out["id"].(string)
}

func fetchTokens(users map[string]bool) map[string]string {
	t0 := time.Now()
	var mu sync.Mutex
	tokens := make(map[string]string, len(users))
	ch := make(chan string)
	var wg sync.WaitGroup
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range ch {
				var tok string
				for attempt := 0; attempt < 5 && tok == ""; attempt++ {
					b, _ := json.Marshal(map[string]string{"user_id": u})
					res, err := httpc.Post(baseURL+"/auth/token", "application/json", bytes.NewReader(b))
					if err != nil {
						time.Sleep(200 * time.Millisecond)
						continue
					}
					var out struct{ Token string }
					_ = json.NewDecoder(res.Body).Decode(&out)
					res.Body.Close()
					tok = out.Token
				}
				if tok == "" {
					fatal("could not get token for %s", u)
				}
				mu.Lock()
				tokens[u] = tok
				mu.Unlock()
			}
		}()
	}
	for u := range users {
		ch <- u
	}
	close(ch)
	wg.Wait()
	fmt.Printf("fetched %d auth tokens in %s\n", len(tokens), time.Since(t0).Round(time.Millisecond))
	return tokens
}

type counts struct {
	Available int `json:"available"`
	Held      int `json:"held"`
	Confirmed int `json:"confirmed"`
}

type showState struct {
	TotalSeats int    `json:"total_seats"`
	Counts     counts `json:"counts"`
	Seats      []struct {
		Label  string `json:"label"`
		Status string `json:"status"`
	} `json:"seats"`
}

func getShow(id string) showState {
	var s showState
	res, err := httpc.Get(baseURL + "/shows/" + id)
	if err != nil {
		fatal("get show: %v", err)
	}
	defer res.Body.Close()
	_ = json.NewDecoder(res.Body).Decode(&s)
	return s
}

func getJSON(path string) map[string]any {
	out := map[string]any{}
	res, err := httpc.Get(baseURL + path)
	if err != nil {
		return out
	}
	defer res.Body.Close()
	_ = json.NewDecoder(res.Body).Decode(&out)
	return out
}

type pollStats struct{ polls, violations, errors5xx int }

// pollInvariant reads GET /shows/{id} continuously during the burst and checks
// both the reported counts and a recount of the per-seat statuses.
func pollInvariant(id string, total int, stop chan struct{}, out chan pollStats) {
	var st pollStats
	for {
		select {
		case <-stop:
			out <- st
			return
		default:
		}
		res, err := httpc.Get(baseURL + "/shows/" + id)
		if err == nil {
			var s showState
			_ = json.NewDecoder(res.Body).Decode(&s)
			res.Body.Close()
			if res.StatusCode >= 500 {
				st.errors5xx++
			} else if res.StatusCode == 200 {
				st.polls++
				recount := map[string]int{}
				for _, seat := range s.Seats {
					recount[seat.Status]++
				}
				c := s.Counts
				if c.Available+c.Held+c.Confirmed != total || len(s.Seats) != total ||
					recount["available"] != c.Available || recount["confirmed"] != c.Confirmed || recount["held"] != c.Held {
					st.violations++
					fmt.Printf("  !! invariant violation: %+v\n", c)
				}
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
}

type metricsSnap struct {
	ok        bool
	available float64
	confirmed float64
	declined  map[string]float64
}

func scrapeMetrics(showID string) metricsSnap {
	m := metricsSnap{declined: map[string]float64{}}
	res, err := httpc.Get(baseURL + "/metrics")
	if err != nil {
		return m
	}
	defer res.Body.Close()
	sc := bufio.NewScanner(res.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	tag := `show_id="` + showID + `"`
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, tag) {
			continue
		}
		fields := strings.Fields(line)
		v, _ := strconv.ParseFloat(fields[len(fields)-1], 64)
		switch {
		case strings.HasPrefix(line, "seats_available{"):
			m.available = v
			m.ok = true
		case strings.HasPrefix(line, "reservations_confirmed_total{"):
			m.confirmed = v
		case strings.HasPrefix(line, "reservations_declined_total{"):
			i := strings.Index(line, `reason="`) + len(`reason="`)
			m.declined[line[i:i+strings.Index(line[i:], `"`)]] = v
		}
	}
	return m
}

// ---------------------------------------------------------------- release

var releaseDelta, releaseConfirms int

// releaseCheck cancels the winner of one hot seat, proves a non-owner can't,
// proves the seat is re-bookable, and proves the stale cancel can't free it.
func releaseCheck(showID string, plan []*plannedReq, seat string, tokens map[string]string) bool {
	var winner *plannedReq
	for _, p := range plan {
		if p.kind == kHot && p.code == 201 && p.seats[0] == seat {
			winner = p
		}
	}
	if winner == nil {
		fmt.Println("no winner found for", seat)
		return false
	}
	var intruder string
	for u := range tokens {
		if u != winner.user {
			intruder = u
			break
		}
	}
	cancel := func(tok string) int {
		req, _ := http.NewRequest("POST", baseURL+"/reservations/"+winner.resID+"/cancel", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		res, err := httpc.Do(req)
		if err != nil {
			return 0
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		return res.StatusCode
	}
	ok := true
	if c := cancel(tokens[intruder]); c != 403 {
		fmt.Printf("  non-owner cancel returned %d (want 403)\n", c)
		ok = false
	}
	if c := cancel(tokens[winner.user]); c != 200 {
		fmt.Printf("  owner cancel returned %d (want 200)\n", c)
		ok = false
	}
	releaseDelta -= len(winner.seats)
	rebook := &plannedReq{user: intruder, key: "rebook-" + seat, seats: []string{seat}}
	rebook.body, _ = json.Marshal(map[string]any{"seats": rebook.seats, "idempotency_key": rebook.key})
	fire(rebook, showID, tokens[intruder])
	if rebook.code != 201 {
		fmt.Printf("  rebook of released seat returned %d (want 201)\n", rebook.code)
		ok = false
	} else {
		releaseDelta++
		releaseConfirms++
	}
	if c := cancel(tokens[winner.user]); c != 200 { // stale second cancel: no-op
		ok = false
	}
	for _, s := range getShow(showID).Seats {
		if s.Label == seat && s.Status != "confirmed" {
			fmt.Println("  stale cancel resurrected the seat!")
			ok = false
		}
	}
	fmt.Printf("cancelled %s (owner %s): non-owner 403, re-booked by %s, stale cancel left it confirmed: %v\n", seat, winner.user, intruder, ok)
	return ok
}

// ---------------------------------------------------------------- analysis

type report struct {
	codes              map[int]int
	reasons            map[string]int
	byKind             map[kind]map[string]int
	fivexx, netErrs    int
	hotWins            map[string]int
	hotDeclines        map[string]int
	hotOther           map[string]int
	confirmedSeats     int
	retryViolations    int
	conflictViolations int
	conflict409        int
	limitViolations    int
	maxPerLimitUser    int
	minPerLimitUser    int
	spoofViolations    int
	latencies          []time.Duration
}

func analyse(plan []*plannedReq, hot []string, limit int) *report {
	r := &report{
		codes: map[int]int{}, reasons: map[string]int{}, byKind: map[kind]map[string]int{},
		hotWins: map[string]int{}, hotDeclines: map[string]int{}, hotOther: map[string]int{},
	}
	retry := map[int][]*plannedReq{}
	conflict := map[int][]*plannedReq{}
	limitWins := map[int]int{}
	limitUserCount := 0
	for _, p := range plan {
		if p.kind == kLimit && p.group+1 > limitUserCount {
			limitUserCount = p.group + 1
		}
		if r.byKind[p.kind] == nil {
			r.byKind[p.kind] = map[string]int{}
		}
		outcome := strconv.Itoa(p.code)
		if p.netErr != "" {
			r.netErrs++
			outcome = "network_error"
		} else {
			r.latencies = append(r.latencies, p.latency)
			r.codes[p.code]++
			if p.errCode != "" {
				r.reasons[p.errCode]++
				outcome += " " + p.errCode
			} else if p.code == 200 {
				outcome += " idempotent_replay"
			} else if p.code == 201 {
				outcome += " confirmed"
			}
		}
		r.byKind[p.kind][outcome]++
		if p.code >= 500 {
			r.fivexx++
		}
		if p.code == 201 {
			r.confirmedSeats += len(p.resSeats)
		}
		switch p.kind {
		case kHot:
			s := p.seats[0]
			switch {
			case p.code == 201:
				r.hotWins[s]++
			case p.code == 409:
				r.hotDeclines[s]++
			default:
				r.hotOther[s]++
			}
		case kRetry:
			retry[p.group] = append(retry[p.group], p)
		case kConflict:
			conflict[p.group] = append(conflict[p.group], p)
		case kLimit:
			if p.code == 201 {
				limitWins[p.group]++
			}
		case kSpoof:
			if (p.code == 201 || p.code == 200) && p.resUser != p.user {
				r.spoofViolations++
			}
		}
	}
	for _, g := range retry {
		wins, ids := 0, map[string]bool{}
		for _, p := range g {
			if p.code == 201 {
				wins++
			}
			if p.resID != "" {
				ids[p.resID] = true
			}
		}
		if wins > 1 || len(ids) > 1 {
			r.retryViolations++
		}
	}
	for _, g := range conflict {
		wins := 0
		for _, p := range g {
			if p.code == 201 {
				wins++
			}
			if p.errCode == "idempotency_key_conflict" {
				r.conflict409++
			}
		}
		if wins > 1 {
			r.conflictViolations++
		}
	}
	r.minPerLimitUser = limit
	for g := 0; g < limitUserCount; g++ {
		w := limitWins[g]
		r.maxPerLimitUser = max(r.maxPerLimitUser, w)
		r.minPerLimitUser = min(r.minPerLimitUser, w)
		// Each user's seats are private, so exactly `limit` must win.
		if w != limit {
			r.limitViolations++
		}
	}
	return r
}

func (r *report) print() {
	fmt.Println("status codes:")
	for _, c := range sortedKeys(r.codes) {
		fmt.Printf("  %d: %d\n", c, r.codes[c])
	}
	if r.netErrs > 0 {
		fmt.Printf("  network errors: %d\n", r.netErrs)
	}
	fmt.Println("declines by reason:")
	for _, k := range sortedStrKeys(r.reasons) {
		fmt.Printf("  %-26s %d\n", k, r.reasons[k])
	}
	fmt.Printf("  %-26s %d\n", "idempotent_replay (200)", r.codes[200])
	fmt.Println("by scenario:")
	kinds := []kind{kHot, kRetry, kConflict, kLimit, kSpoof, kCrowd}
	for _, k := range kinds {
		parts := []string{}
		for _, o := range sortedStrKeys(r.byKind[k]) {
			parts = append(parts, fmt.Sprintf("%s=%d", o, r.byKind[k][o]))
		}
		fmt.Printf("  %-20s %s\n", k, strings.Join(parts, "  "))
	}
	if len(r.latencies) > 0 {
		sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
		q := func(p float64) time.Duration {
			return r.latencies[int(p*float64(len(r.latencies)-1))].Round(time.Millisecond)
		}
		fmt.Printf("latency: p50=%s p90=%s p99=%s max=%s\n", q(.5), q(.9), q(.99), q(1))
	}
}

// ---------------------------------------------------------------- misc

type check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

func writeReport(path, showID string, elapsed time.Duration, r *report, final showState, audit map[string]any, checks []check) {
	byKind := map[string]map[string]int{}
	for k, v := range r.byKind {
		byKind[string(k)] = v
	}
	b, _ := json.MarshalIndent(map[string]any{
		"base_url": baseURL, "show_id": showID, "elapsed_ms": elapsed.Milliseconds(),
		"status_codes": r.codes, "declines": r.reasons, "by_scenario": byKind,
		"network_errors": r.netErrs, "final_counts": final.Counts, "audit": audit, "checks": checks,
	}, "", "  ")
	_ = os.WriteFile(path, b, 0o644)
	fmt.Println("wrote", path)
}

func sortedKeys(m map[int]int) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

func sortedStrKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func section(s string) { fmt.Printf("\n=== %s ===\n", s) }

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", a...)
	os.Exit(1)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
