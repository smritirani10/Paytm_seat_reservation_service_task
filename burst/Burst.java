// Reproduces the on-sale stampede against a running deployment and verifies
// the correctness bar from the outside. Single file, JDK only, no build step:
//
//   java burst/Burst.java -url https://host [-requests 20000] [-concurrency 20000]
//
// Checks:
//   - hot-seat storm: many users, same seat -> exactly one 201, rest 409
//   - zero 5xx across the whole burst
//   - available+held+confirmed == total_seats during and after the burst
//   - idempotent retries create one reservation; same key + other seats -> 409
//   - a user firing 10 parallel reserves on a limit=4 show ends with exactly 4
//   - a spoofed body user_id never changes who the seat is booked for
//   - release: owner cancel frees the seat, non-owner gets 403, stale cancel is a no-op
//   - metrics reconcile with what the client observed and with GET /shows/{id}

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Burst {

    enum Kind {
        HOT("hot-seat-storm"), RETRY("idempotent-retry"), CONFLICT("same-key-diff-body"),
        LIMIT("per-user-limit"), SPOOF("spoofed-user"), CROWD("general-crowd");
        final String label;
        Kind(String l) { label = l; }
    }

    static final class Req {
        Kind kind;
        int group;
        String user, key;
        List<String> seats;
        String body;
        // results
        int code;
        String resId, resUser, errCode, netErr;
        List<String> resSeats = List.of();
        long latencyNanos;
    }

    static String baseUrl;
    static String adminToken;
    static HttpClient http;
    static Duration timeout;

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = parseArgs(argv);
        baseUrl = a.getOrDefault("url", System.getenv().getOrDefault("BASE_URL", ""));
        if (baseUrl.isEmpty()) {
            System.err.println("usage: java burst/Burst.java -url <BASE_URL> [-requests N] [-concurrency N] ...");
            System.exit(2);
        }
        baseUrl = baseUrl.replaceAll("/+$", "");
        adminToken = a.getOrDefault("admin-token", System.getenv().getOrDefault("ADMIN_TOKEN", "dev-admin-token"));
        int requests = intArg(a, "requests", 20000);
        int concurrency = intArg(a, "concurrency", 20000);
        int nSeats = intArg(a, "seats", 2500);
        int hot = intArg(a, "hot", 5);
        int storm = intArg(a, "storm", 500);
        int retryGroups = intArg(a, "retry-groups", 1000);
        int conflicts = intArg(a, "conflict-groups", 200);
        int limitUsers = intArg(a, "limit-users", 100);
        int spoofs = intArg(a, "spoofs", 100);
        int limit = intArg(a, "limit", 4);
        timeout = Duration.ofSeconds(intArg(a, "timeout", 90));
        String jsonOut = a.get("json");

        http = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(Duration.ofSeconds(30))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        section("1. setup");
        waitReady();
        List<String> labels = seatLabels(nSeats);
        String showId = createShow(labels, limit);
        System.out.printf("show %s: %d seats, per_user_limit=%d, price 25000 paise%n", showId, nSeats, limit);

        List<String> hotSeats = pickHot(labels, hot);
        Set<String> isHot = new HashSet<>(hotSeats);
        List<String> cold = labels.stream().filter(l -> !isHot.contains(l)).toList();
        // Limit-test users get a private block of 10 seats each, so the only thing
        // that can stop them is the per-user limit (not a sold-out show).
        if (cold.size() < limitUsers * 10 + 100) fatal("need at least %d seats for this plan", hot + limitUsers * 10 + 100);
        List<String> limitBlock = cold.subList(0, limitUsers * 10);
        List<String> common = cold.subList(limitUsers * 10, cold.size());

        List<Req> plan = buildPlan(requests, hotSeats, limitBlock, common, storm, retryGroups, conflicts, limitUsers, spoofs);
        Set<String> users = new HashSet<>();
        for (Req r : plan) users.add(r.user);
        System.out.printf("planned %d reserve requests from %d distinct users; hot seats %s%n", plan.size(), users.size(), hotSeats);
        Map<String, String> tokens = fetchTokens(users);

        for (Req r : plan) {
            StringBuilder b = new StringBuilder("{\"seats\":").append(jsonArray(r.seats))
                    .append(",\"idempotency_key\":").append(jsonString(r.key));
            if (r.kind == Kind.SPOOF) b.append(",\"user_id\":\"victim-user\"");
            r.body = b.append('}').toString();
        }

        section("2. burst");
        PollStats polls = new PollStats();
        Thread poller = Thread.ofVirtual().start(() -> pollInvariant(showId, nSeats, polls));

        Semaphore sem = new Semaphore(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger done = new AtomicInteger();
        long t0 = System.nanoTime();
        try (ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Req r : plan) {
                ex.submit(() -> {
                    start.await();
                    sem.acquire();
                    try {
                        fire(r, showId, tokens.get(r.user));
                    } finally {
                        sem.release();
                        done.incrementAndGet();
                    }
                    return null;
                });
            }
            Thread progress = Thread.ofVirtual().start(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try { Thread.sleep(1000); } catch (InterruptedException e) { return; }
                    System.out.printf("  ... %d/%d done (%.0fs)%n", done.get(), plan.size(), (System.nanoTime() - t0) / 1e9);
                }
            });
            start.countDown();
            ex.shutdown();
            ex.awaitTermination(1, TimeUnit.HOURS);
            progress.interrupt();
        }
        double elapsed = (System.nanoTime() - t0) / 1e9;
        polls.stop = true;
        poller.join();
        System.out.printf("burst finished: %d requests in %.3fs (%.0f req/s)%n", plan.size(), elapsed, plan.size() / elapsed);

        section("3. outcome distribution");
        Report rep = analyse(plan, limit);
        rep.print();

        section("4. release check");
        Release rel = releaseCheck(showId, plan, hotSeats.get(0), tokens);

        section("5. final reconciliation");
        Map<String, Object> fin = getJson("/shows/" + showId);
        Map<String, Object> audit = getJson("/shows/" + showId + "/audit");
        MetricsSnap met = scrapeMetrics(showId);
        Map<String, Object> counts = obj(fin.get("counts"));
        int avail = num(counts.get("available")), held = num(counts.get("held")), conf = num(counts.get("confirmed"));

        List<Check> checks = new ArrayList<>();
        for (String h : hotSeats) {
            int w = rep.hotWins.getOrDefault(h, 0);
            checks.add(new Check("hot seat " + h + ": exactly one 201", w == 1,
                    String.format("201=%d 409=%d other=%d", w, rep.hotDeclines.getOrDefault(h, 0), rep.hotOther.getOrDefault(h, 0))));
        }
        checks.add(new Check("zero 5xx across the burst", rep.fivexx == 0, String.format("5xx=%d network_errors=%d", rep.fivexx, rep.netErrs)));
        checks.add(new Check("invariant held during burst", polls.violations.get() == 0 && polls.errors5xx.get() == 0,
                String.format("%d polls, %d violations, %d poll 5xx", polls.polls.get(), polls.violations.get(), polls.errors5xx.get())));
        checks.add(new Check("invariant holds after burst", avail + held + conf == nSeats,
                String.format("available=%d held=%d confirmed=%d total=%d", avail, held, conf, nSeats)));
        checks.add(new Check("idempotent retries: one reservation per key", rep.retryViolations == 0,
                String.format("%d groups, %d violations", retryGroups, rep.retryViolations)));
        checks.add(new Check("same key + different seats -> 409", rep.conflictViolations == 0 && rep.conflict409 > 0,
                String.format("%d idempotency_key_conflict, %d violations", rep.conflict409, rep.conflictViolations)));
        checks.add(new Check("per-user limit: 10 parallel reserves -> exactly limit confirmed", rep.limitViolations == 0,
                String.format("%d users; 201s per user min=%d max=%d (limit %d)", limitUsers, rep.minPerLimitUser, rep.maxPerLimitUser, limit)));
        checks.add(new Check("spoofed user_id ignored (token identity wins)", rep.spoofViolations == 0,
                String.format("%d spoofed requests, %d violations", spoofs, rep.spoofViolations)));
        checks.add(new Check("server-side audit ok", Boolean.TRUE.equals(audit.get("ok")),
                String.format("max_seats_per_user=%s orphan_seats=%s broken_reservations=%s",
                        audit.get("max_seats_per_user"), audit.get("orphan_seats"), audit.get("broken_reservations"))));
        checks.add(new Check("release: cancelled seat is re-bookable, non-owner cannot cancel", rel.ok, ""));
        int expectSeats = rep.confirmedSeats + rel.seatDelta;
        checks.add(new Check("confirmed seats == seats in 201 responses", conf == expectSeats,
                String.format("api=%d client_observed=%d", conf, expectSeats)));
        if (met.ok) {
            int observed201 = rep.codes.getOrDefault(201, 0) + rel.confirms;
            checks.add(new Check("metric seats_available == API available", (int) met.available == avail,
                    String.format("metric=%.0f api=%d", met.available, avail)));
            checks.add(new Check("metric reservations_confirmed_total == observed 201s", (int) met.confirmed == observed201,
                    String.format("metric=%.0f observed=%d", met.confirmed, observed201)));
            checks.add(metricCheck(met, "seat_taken", rep.reasons.getOrDefault("seat_taken", 0)));
            checks.add(metricCheck(met, "per_user_limit", rep.reasons.getOrDefault("per_user_limit_exceeded", 0)));
            checks.add(metricCheck(met, "idempotent_replay", rep.codes.getOrDefault(200, 0)));
        } else {
            checks.add(new Check("metrics scraped", false, "could not read /metrics"));
        }

        int failed = 0;
        for (Check c : checks) {
            if (!c.ok) failed++;
            System.out.printf("  [%s] %-55s %s%n", c.ok ? "PASS" : "FAIL", c.name, c.detail);
        }
        if (rep.netErrs > 0) {
            System.out.printf("%n  note: %d client-side network errors/timeouts (no HTTP status received); counter reconciliation may be off by those.%n", rep.netErrs);
        }
        if (jsonOut != null) writeReport(jsonOut, showId, elapsed, rep, counts, audit, checks);

        section("RESULT");
        if (failed > 0) {
            System.out.printf("%d of %d checks FAILED%n", failed, checks.size());
            System.exit(1);
        }
        System.out.printf("all %d checks passed%n", checks.size());
        System.exit(0);
    }

    // ------------------------------------------------------------------ plan

    static List<Req> buildPlan(int total, List<String> hot, List<String> limitBlock, List<String> cold, int storm,
                               int retryGroups, int conflicts, int limitUsers, int spoofs) {
        Random rng = new Random();
        String run = Long.toString(Instant.now().getEpochSecond() % 1_000_000, 36);
        List<Req> plan = new ArrayList<>(total);
        for (int h = 0; h < hot.size(); h++) {
            for (int i = 0; i < storm; i++) plan.add(req(Kind.HOT, h, "hot-" + run + "-" + h + "-" + i, "k-" + hot.get(h), List.of(hot.get(h))));
        }
        for (int g = 0; g < retryGroups; g++) {
            List<String> seats = List.of(cold.get(rng.nextInt(cold.size())));
            for (int c = 0; c < 3; c++) plan.add(req(Kind.RETRY, g, "retry-" + run + "-" + g, "retry-key", seats));
        }
        for (int g = 0; g < conflicts; g++) {
            String x = cold.get(rng.nextInt(cold.size())), y;
            do { y = cold.get(rng.nextInt(cold.size())); } while (y.equals(x));
            String u = "conflict-" + run + "-" + g;
            plan.add(req(Kind.CONFLICT, g, u, "conflict-key", List.of(x)));
            plan.add(req(Kind.CONFLICT, g, u, "conflict-key", List.of(y)));
        }
        for (int g = 0; g < limitUsers; g++) {
            for (int i = 0; i < 10; i++) plan.add(req(Kind.LIMIT, g, "limit-" + run + "-" + g, "limit-" + i, List.of(limitBlock.get(g * 10 + i))));
        }
        for (int i = 0; i < spoofs; i++) {
            plan.add(req(Kind.SPOOF, i, "spoof-" + run + "-" + i, "spoof", List.of(cold.get(rng.nextInt(cold.size())))));
        }
        int crowdUsers = 4000;
        for (int i = 0; plan.size() < total; i++) {
            int n = 1 + rng.nextInt(3);
            LinkedHashSet<String> seats = new LinkedHashSet<>();
            while (seats.size() < n) seats.add(cold.get(rng.nextInt(cold.size())));
            plan.add(req(Kind.CROWD, i, "crowd-" + run + "-" + rng.nextInt(crowdUsers), "crowd-" + i, new ArrayList<>(seats)));
        }
        Collections.shuffle(plan, rng);
        return plan;
    }

    static Req req(Kind k, int g, String user, String key, List<String> seats) {
        Req r = new Req();
        r.kind = k;
        r.group = g;
        r.user = user;
        r.key = key;
        r.seats = seats;
        return r;
    }

    static List<String> seatLabels(int n) {
        int perRow = 25;
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            StringBuilder name = new StringBuilder();
            for (int r = i / perRow; ; r = r / 26 - 1) {
                name.insert(0, (char) ('A' + r % 26));
                if (r < 26) break;
            }
            out.add(name + Integer.toString(i % perRow + 1));
        }
        return out;
    }

    static List<String> pickHot(List<String> labels, int n) {
        int start = 11; // A12, A13, ... the "good" seats
        if (start + n > labels.size()) start = 0;
        return new ArrayList<>(labels.subList(start, Math.min(labels.size(), start + n)));
    }

    // ------------------------------------------------------------------ http

    record HttpResult(int code, String body) {}

    static HttpResult send(String method, String path, String token, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(timeout)
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) b.header("Content-Type", "application/json");
        if (token != null) b.header("Authorization", "Bearer " + token);
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        return new HttpResult(r.statusCode(), r.body());
    }

    static void fire(Req r, String showId, String token) {
        long t = System.nanoTime();
        try {
            HttpResult res = send("POST", "/shows/" + showId + "/reserve", token, r.body);
            r.code = res.code();
            Map<String, Object> b = parseObj(res.body());
            r.resId = str(b.get("reservation_id"));
            r.resUser = str(b.get("user_id"));
            r.errCode = str(b.get("error"));
            if (b.get("seats") instanceof List<?> l && r.code < 300) r.resSeats = l.stream().map(String::valueOf).toList();
        } catch (Exception e) {
            r.netErr = String.valueOf(e);
        } finally {
            r.latencyNanos = System.nanoTime() - t;
        }
    }

    static void waitReady() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 180_000;
        while (true) {
            try {
                if (send("GET", "/readyz", null, null).code() == 200) {
                    System.out.println("service is ready");
                    return;
                }
            } catch (Exception ignored) {
                // not up yet
            }
            if (System.currentTimeMillis() > deadline) fatal("service never became ready at %s/readyz", baseUrl);
            System.out.println("waiting for /readyz (cold start?) ...");
            Thread.sleep(3000);
        }
    }

    static String createShow(List<String> labels, int limit) throws Exception {
        String name = "burst-" + DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC).format(Instant.now());
        String body = "{\"name\":" + jsonString(name) + ",\"seats\":" + jsonArray(labels) + ",\"price_paise\":25000,\"per_user_limit\":" + limit + "}";
        HttpResult r = send("POST", "/shows", adminToken, body);
        if (r.code() != 201) fatal("create show: HTTP %d %s (is ADMIN_TOKEN right?)", r.code(), r.body());
        return str(parseObj(r.body()).get("id"));
    }

    static Map<String, String> fetchTokens(Set<String> users) throws Exception {
        long t0 = System.nanoTime();
        Map<String, String> tokens = new ConcurrentHashMap<>();
        Semaphore sem = new Semaphore(64);
        try (ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String u : users) {
                ex.submit(() -> {
                    sem.acquire();
                    try {
                        for (int attempt = 0; attempt < 5; attempt++) {
                            try {
                                HttpResult r = send("POST", "/auth/token", null, "{\"user_id\":" + jsonString(u) + "}");
                                String tok = str(parseObj(r.body()).get("token"));
                                if (tok != null) {
                                    tokens.put(u, tok);
                                    return null;
                                }
                            } catch (Exception e) {
                                Thread.sleep(200);
                            }
                        }
                        fatal("could not get token for %s", u);
                    } finally {
                        sem.release();
                    }
                    return null;
                });
            }
        }
        System.out.printf("fetched %d auth tokens in %dms%n", tokens.size(), (System.nanoTime() - t0) / 1_000_000);
        return tokens;
    }

    static Map<String, Object> getJson(String path) {
        try {
            return parseObj(send("GET", path, null, null).body());
        } catch (Exception e) {
            return Map.of();
        }
    }

    static final class PollStats {
        volatile boolean stop;
        final AtomicInteger polls = new AtomicInteger(), violations = new AtomicInteger(), errors5xx = new AtomicInteger();
    }

    /** Reads GET /shows/{id} continuously during the burst; checks reported counts and a recount of per-seat statuses. */
    static void pollInvariant(String id, int total, PollStats st) {
        while (!st.stop) {
            try {
                HttpResult r = send("GET", "/shows/" + id, null, null);
                if (r.code() >= 500) {
                    st.errors5xx.incrementAndGet();
                } else if (r.code() == 200) {
                    st.polls.incrementAndGet();
                    Map<String, Object> s = parseObj(r.body());
                    Map<String, Object> c = obj(s.get("counts"));
                    int av = num(c.get("available")), he = num(c.get("held")), co = num(c.get("confirmed"));
                    Map<String, Integer> recount = new HashMap<>();
                    List<?> seats = (List<?>) s.get("seats");
                    for (Object o : seats) recount.merge(str(obj(o).get("status")), 1, Integer::sum);
                    if (av + he + co != total || seats.size() != total || recount.getOrDefault("available", 0) != av
                            || recount.getOrDefault("confirmed", 0) != co || recount.getOrDefault("held", 0) != he) {
                        st.violations.incrementAndGet();
                        System.out.println("  !! invariant violation: " + c);
                    }
                }
            } catch (Exception ignored) {
                // network hiccup on the poller: not a server verdict
            }
            try { Thread.sleep(150); } catch (InterruptedException e) { return; }
        }
    }

    static final class MetricsSnap {
        boolean ok;
        double available, confirmed;
        Map<String, Double> declined = new HashMap<>();
    }

    static MetricsSnap scrapeMetrics(String showId) {
        MetricsSnap m = new MetricsSnap();
        String text;
        try {
            text = send("GET", "/metrics", null, null).body();
        } catch (Exception e) {
            return m;
        }
        String tag = "show_id=\"" + showId + "\"";
        for (String line : text.split("\n")) {
            if (!line.contains(tag) || line.startsWith("#")) continue;
            double v = Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
            if (line.startsWith("seats_available{")) {
                m.available = v;
                m.ok = true;
            } else if (line.startsWith("reservations_confirmed_total{")) {
                m.confirmed = v;
            } else if (line.startsWith("reservations_declined_total{")) {
                int i = line.indexOf("reason=\"") + 8;
                m.declined.put(line.substring(i, line.indexOf('"', i)), v);
            }
        }
        return m;
    }

    static Check metricCheck(MetricsSnap m, String reason, int observed) {
        double v = m.declined.getOrDefault(reason, 0.0);
        return new Check("metric declined{" + reason + "} == observed", (int) v == observed, String.format("metric=%.0f observed=%d", v, observed));
    }

    // ------------------------------------------------------------------ release

    record Release(boolean ok, int seatDelta, int confirms) {}

    /** Cancels the winner of one hot seat, proves a non-owner can't, proves it's re-bookable, and that a stale cancel can't free it. */
    static Release releaseCheck(String showId, List<Req> plan, String seat, Map<String, String> tokens) throws Exception {
        Req winner = null;
        for (Req r : plan) if (r.kind == Kind.HOT && r.code == 201 && r.seats.get(0).equals(seat)) winner = r;
        if (winner == null) {
            System.out.println("no winner found for " + seat);
            return new Release(false, 0, 0);
        }
        String owner = winner.user;
        String intruder = tokens.keySet().stream().filter(u -> !u.equals(owner)).findFirst().orElseThrow();
        String path = "/reservations/" + winner.resId + "/cancel";
        boolean ok = true;
        int delta = 0, confirms = 0;
        int c = send("POST", path, tokens.get(intruder), null).code();
        if (c != 403) { System.out.printf("  non-owner cancel returned %d (want 403)%n", c); ok = false; }
        c = send("POST", path, tokens.get(owner), null).code();
        if (c != 200) { System.out.printf("  owner cancel returned %d (want 200)%n", c); ok = false; }
        delta -= winner.seats.size();
        Req rebook = req(Kind.HOT, 0, intruder, "rebook-" + seat, List.of(seat));
        rebook.body = "{\"seats\":" + jsonArray(rebook.seats) + ",\"idempotency_key\":" + jsonString(rebook.key) + "}";
        fire(rebook, showId, tokens.get(intruder));
        if (rebook.code != 201) {
            System.out.printf("  rebook of released seat returned %d (want 201)%n", rebook.code);
            ok = false;
        } else {
            delta++;
            confirms++;
        }
        if (send("POST", path, tokens.get(owner), null).code() != 200) ok = false; // stale second cancel: no-op
        for (Object o : (List<?>) getJson("/shows/" + showId).get("seats")) {
            Map<String, Object> s = obj(o);
            if (seat.equals(s.get("label")) && !"confirmed".equals(s.get("status"))) {
                System.out.println("  stale cancel resurrected the seat!");
                ok = false;
            }
        }
        System.out.printf("cancelled %s (owner %s): non-owner 403, re-booked by %s, stale cancel left it confirmed: %s%n", seat, owner, intruder, ok);
        return new Release(ok, delta, confirms);
    }

    // ------------------------------------------------------------------ analysis

    static final class Report {
        Map<Integer, Integer> codes = new TreeMap<>();
        Map<String, Integer> reasons = new TreeMap<>();
        Map<Kind, Map<String, Integer>> byKind = new EnumMap<>(Kind.class);
        int fivexx, netErrs, confirmedSeats, retryViolations, conflictViolations, conflict409,
                limitViolations, maxPerLimitUser, minPerLimitUser, spoofViolations;
        Map<String, Integer> hotWins = new HashMap<>(), hotDeclines = new HashMap<>(), hotOther = new HashMap<>();
        List<Long> latencies = new ArrayList<>();

        void print() {
            System.out.println("status codes:");
            codes.forEach((c, n) -> System.out.printf("  %d: %d%n", c, n));
            if (netErrs > 0) System.out.printf("  network errors: %d%n", netErrs);
            System.out.println("declines by reason:");
            reasons.forEach((k, n) -> System.out.printf("  %-26s %d%n", k, n));
            System.out.printf("  %-26s %d%n", "idempotent_replay (200)", codes.getOrDefault(200, 0));
            System.out.println("by scenario:");
            for (Kind k : Kind.values()) {
                StringJoiner j = new StringJoiner("  ");
                new TreeMap<>(byKind.getOrDefault(k, Map.of())).forEach((o, n) -> j.add(o + "=" + n));
                System.out.printf("  %-20s %s%n", k.label, j);
            }
            if (!latencies.isEmpty()) {
                Collections.sort(latencies);
                System.out.printf("latency: p50=%s p90=%s p99=%s max=%s%n", q(.5), q(.9), q(.99), q(1));
            }
        }

        String q(double p) {
            long ns = latencies.get((int) (p * (latencies.size() - 1)));
            return ns >= 1_000_000_000L ? String.format("%.3fs", ns / 1e9) : String.format("%dms", ns / 1_000_000);
        }
    }

    static Report analyse(List<Req> plan, int limit) {
        Report r = new Report();
        Map<Integer, List<Req>> retry = new HashMap<>(), conflict = new HashMap<>();
        Map<Integer, Integer> limitWins = new HashMap<>();
        int limitUserCount = 0;
        for (Req p : plan) {
            String outcome;
            if (p.netErr != null) {
                r.netErrs++;
                outcome = "network_error";
            } else {
                r.latencies.add(p.latencyNanos);
                r.codes.merge(p.code, 1, Integer::sum);
                outcome = Integer.toString(p.code);
                if (p.errCode != null) {
                    r.reasons.merge(p.errCode, 1, Integer::sum);
                    outcome += " " + p.errCode;
                } else if (p.code == 200) {
                    outcome += " idempotent_replay";
                } else if (p.code == 201) {
                    outcome += " confirmed";
                }
            }
            r.byKind.computeIfAbsent(p.kind, k -> new HashMap<>()).merge(outcome, 1, Integer::sum);
            if (p.code >= 500) r.fivexx++;
            if (p.code == 201) r.confirmedSeats += p.resSeats.size();
            switch (p.kind) {
                case HOT -> {
                    String s = p.seats.get(0);
                    Map<String, Integer> m = p.code == 201 ? r.hotWins : p.code == 409 ? r.hotDeclines : r.hotOther;
                    m.merge(s, 1, Integer::sum);
                }
                case RETRY -> retry.computeIfAbsent(p.group, k -> new ArrayList<>()).add(p);
                case CONFLICT -> conflict.computeIfAbsent(p.group, k -> new ArrayList<>()).add(p);
                case LIMIT -> {
                    limitUserCount = Math.max(limitUserCount, p.group + 1);
                    if (p.code == 201) limitWins.merge(p.group, 1, Integer::sum);
                }
                case SPOOF -> {
                    if ((p.code == 201 || p.code == 200) && !p.user.equals(p.resUser)) r.spoofViolations++;
                }
                default -> { }
            }
        }
        for (List<Req> g : retry.values()) {
            long wins = g.stream().filter(p -> p.code == 201).count();
            long ids = g.stream().map(p -> p.resId).filter(Objects::nonNull).distinct().count();
            if (wins > 1 || ids > 1) r.retryViolations++;
        }
        for (List<Req> g : conflict.values()) {
            if (g.stream().filter(p -> p.code == 201).count() > 1) r.conflictViolations++;
            r.conflict409 += (int) g.stream().filter(p -> "idempotency_key_conflict".equals(p.errCode)).count();
        }
        r.minPerLimitUser = limit;
        for (int g = 0; g < limitUserCount; g++) {
            int w = limitWins.getOrDefault(g, 0);
            r.maxPerLimitUser = Math.max(r.maxPerLimitUser, w);
            r.minPerLimitUser = Math.min(r.minPerLimitUser, w);
            if (w != limit) r.limitViolations++; // each user's seats are private, so exactly `limit` must win
        }
        return r;
    }

    // ------------------------------------------------------------------ misc

    record Check(String name, boolean ok, String detail) {}

    static void writeReport(String path, String showId, double elapsed, Report r, Map<String, Object> counts,
                            Map<String, Object> audit, List<Check> checks) throws Exception {
        StringBuilder b = new StringBuilder("{\n");
        b.append("  \"base_url\": ").append(jsonString(baseUrl)).append(",\n");
        b.append("  \"show_id\": ").append(jsonString(showId)).append(",\n");
        b.append("  \"elapsed_ms\": ").append(Math.round(elapsed * 1000)).append(",\n");
        StringJoiner codes = new StringJoiner(", ", "{", "}");
        r.codes.forEach((c, n) -> codes.add("\"" + c + "\": " + n));
        b.append("  \"status_codes\": ").append(codes).append(",\n");
        StringJoiner reasons = new StringJoiner(", ", "{", "}");
        r.reasons.forEach((k, n) -> reasons.add(jsonString(k) + ": " + n));
        b.append("  \"declines\": ").append(reasons).append(",\n");
        b.append("  \"network_errors\": ").append(r.netErrs).append(",\n");
        b.append("  \"final_counts\": ").append(jsonString(String.valueOf(counts))).append(",\n");
        b.append("  \"audit_ok\": ").append(Boolean.TRUE.equals(audit.get("ok"))).append(",\n");
        StringJoiner cs = new StringJoiner(",\n    ", "[\n    ", "\n  ]");
        for (Check c : checks) cs.add("{\"name\": " + jsonString(c.name) + ", \"ok\": " + c.ok + ", \"detail\": " + jsonString(c.detail) + "}");
        b.append("  \"checks\": ").append(cs).append("\n}\n");
        Files.writeString(Path.of(path), b);
        System.out.println("wrote " + path);
    }

    static void section(String s) {
        System.out.printf("%n=== %s ===%n", s);
    }

    static void fatal(String f, Object... a) {
        System.err.printf("FATAL: " + f + "%n", a);
        System.exit(1);
    }

    static Map<String, String> parseArgs(String[] argv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            if (a.startsWith("-")) {
                String k = a.replaceFirst("^-+", "");
                if (k.contains("=")) {
                    m.put(k.substring(0, k.indexOf('=')), k.substring(k.indexOf('=') + 1));
                } else if (i + 1 < argv.length) {
                    m.put(k, argv[++i]);
                }
            } else if (!m.containsKey("url")) {
                m.put("url", a);
            }
        }
        return m;
    }

    static int intArg(Map<String, String> a, String k, int def) {
        return a.containsKey(k) ? Integer.parseInt(a.get(k)) : def;
    }

    // ------------------------------------------------------------------ minimal JSON (JDK has none built in)

    static String jsonString(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
                }
            }
        }
        return b.append('"').toString();
    }

    static String jsonArray(List<String> items) {
        StringJoiner j = new StringJoiner(",", "[", "]");
        for (String s : items) j.add(jsonString(s));
        return j.toString();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> obj(Object o) {
        return o instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }

    static Map<String, Object> parseObj(String s) {
        try {
            return obj(new JsonParser(s).value());
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    static String str(Object o) {
        return o == null ? null : o.toString();
    }

    static int num(Object o) {
        return o instanceof Number n ? n.intValue() : 0;
    }

    static final class JsonParser {
        private final String s;
        private int i;

        JsonParser(String s) {
            this.s = s;
        }

        Object value() {
            ws();
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> lit("true", Boolean.TRUE);
                case 'f' -> lit("false", Boolean.FALSE);
                case 'n' -> lit("null", null);
                default -> number();
            };
        }

        private Map<String, Object> object() {
            Map<String, Object> m = new LinkedHashMap<>();
            i++;
            ws();
            if (s.charAt(i) == '}') { i++; return m; }
            while (true) {
                ws();
                String k = string();
                ws();
                i++; // ':'
                m.put(k, value());
                ws();
                if (s.charAt(i++) == '}') return m;
            }
        }

        private List<Object> array() {
            List<Object> l = new ArrayList<>();
            i++;
            ws();
            if (s.charAt(i) == ']') { i++; return l; }
            while (true) {
                l.add(value());
                ws();
                if (s.charAt(i++) == ']') return l;
            }
        }

        private String string() {
            StringBuilder b = new StringBuilder();
            i++; // opening quote
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') return b.toString();
                if (c == '\\') {
                    char e = s.charAt(i++);
                    switch (e) {
                        case 'n' -> b.append('\n');
                        case 't' -> b.append('\t');
                        case 'r' -> b.append('\r');
                        case 'b' -> b.append('\b');
                        case 'f' -> b.append('\f');
                        case 'u' -> { b.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; }
                        default -> b.append(e);
                    }
                } else {
                    b.append(c);
                }
            }
        }

        private Object number() {
            int st = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) i++;
            String n = s.substring(st, i);
            if (n.isEmpty()) throw new IllegalArgumentException("bad json at " + st);
            return n.contains(".") || n.contains("e") || n.contains("E") ? (Object) Double.parseDouble(n) : (Object) Long.parseLong(n);
        }

        private Object lit(String word, Object v) {
            if (!s.startsWith(word, i)) throw new IllegalArgumentException("bad json at " + i);
            i += word.length();
            return v;
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }
    }
}
