package com.paytm.seats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paytm.seats.auth.Authenticator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests against a real Postgres (TEST_DATABASE_URL), because the
 * point is exercising the database's locking under real concurrency.
 */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "DATABASE_URL=${TEST_DATABASE_URL}", "ADMIN_TOKEN=test-admin", "JWT_SECRET=test-secret", "DB_MAX_CONNS=30"})
class ReservationIntegrationTest {
    private static final String ADMIN = "test-admin";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .executor(Executors.newVirtualThreadPerTaskExecutor()).connectTimeout(Duration.ofSeconds(10)).build();

    @LocalServerPort
    int port;

    @Autowired
    Authenticator auth;

    /** Unique per test so reruns never collide on per-user idempotency keys. */
    String run;

    record Resp(int code, JsonNode body) {
        String str(String f) {
            return body.path(f).asText(null);
        }
    }

    @BeforeEach
    void waitReady() throws Exception {
        run = Long.toString(System.nanoTime(), 36);
        for (int i = 0; i < 100; i++) {
            if (call("GET", "/readyz", null, null).code() == 200) return;
            Thread.sleep(100);
        }
        fail("service never became ready");
    }

    // ------------------------------------------------------------------ helpers

    Resp call(String method, String path, String token, Object body) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                    .timeout(Duration.ofSeconds(60))
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                            : HttpRequest.BodyPublishers.ofString(body instanceof String s ? s : JSON.writeValueAsString(body)));
            if (token != null) b.header("Authorization", "Bearer " + token);
            HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
            return new Resp(r.statusCode(), r.body().isEmpty() ? JSON.nullNode() : JSON.readTree(r.body()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    String token(String user) {
        return auth.issue(user + "-" + run).token();
    }

    String user(String user) {
        return user + "-" + run;
    }

    String newShow(int n, int limit) {
        List<String> seats = new ArrayList<>();
        for (int i = 1; i <= n; i++) seats.add("S" + i);
        Resp r = call("POST", "/shows", ADMIN, Map.of("name", "test", "seats", seats, "price_paise", 25000, "per_user_limit", limit));
        assertEquals(201, r.code(), r.body().toString());
        return r.str("id");
    }

    Resp reserve(String show, String tok, String key, String... seats) {
        return call("POST", "/shows/" + show + "/reserve", tok, Map.of("seats", List.of(seats), "idempotency_key", key));
    }

    JsonNode assertAudit(String show) {
        Resp r = call("GET", "/shows/" + show + "/audit", null, null);
        assertEquals(200, r.code());
        assertTrue(r.body().path("ok").asBoolean(), "audit failed: " + r.body());
        return r.body();
    }

    static int confirmed(JsonNode audit) {
        return audit.path("counts").path("confirmed").asInt();
    }

    /** Runs n tasks released at the same instant. */
    static void parallel(int n, IntConsumer task) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> fs = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int idx = i;
                fs.add(ex.submit(() -> {
                    start.await();
                    task.accept(idx);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : fs) f.get();
        }
    }

    // ------------------------------------------------------------------ tests

    @Test
    void hotSeatHasExactlyOneWinner() throws Exception {
        String show = newShow(10, 4);
        Map<Integer, AtomicInteger> codes = new ConcurrentHashMap<>();
        parallel(500, i -> codes.computeIfAbsent(reserve(show, token("hot-" + i), "k", "S1").code(), k -> new AtomicInteger()).incrementAndGet());
        assertEquals(1, codes.getOrDefault(201, new AtomicInteger()).get(), codes.toString());
        assertEquals(499, codes.getOrDefault(409, new AtomicInteger()).get(), codes.toString());
        assertEquals(1, confirmed(assertAudit(show)));
    }

    @Test
    void perUserLimitHoldsUnderConcurrency() throws Exception {
        String show = newShow(20, 4);
        String tok = token("greedy");
        AtomicInteger ok = new AtomicInteger();
        List<String> unexpected = Collections.synchronizedList(new ArrayList<>());
        parallel(10, i -> {
            Resp r = reserve(show, tok, "k" + i, "S" + (i + 1));
            if (r.code() == 201) ok.incrementAndGet();
            else if (r.code() != 409 || !"per_user_limit_exceeded".equals(r.str("error"))) unexpected.add(r.code() + " " + r.body());
        });
        assertEquals(List.of(), unexpected);
        assertEquals(4, ok.get());
        assertEquals(4, assertAudit(show).path("max_seats_per_user").asInt());
        // A multi-seat request that would cross the limit is declined whole.
        String show2 = newShow(20, 4);
        assertEquals(409, reserve(show2, tok, "big", "S1", "S2", "S3", "S4", "S5").code());
    }

    @Test
    void idempotentRetriesReserveOnce() throws Exception {
        String show = newShow(10, 4);
        String tok = token("retrier");
        Map<Integer, AtomicInteger> codes = new ConcurrentHashMap<>();
        Set<String> ids = ConcurrentHashMap.newKeySet();
        parallel(50, i -> {
            Resp r = reserve(show, tok, "same-key", "S3", "S4");
            codes.computeIfAbsent(r.code(), k -> new AtomicInteger()).incrementAndGet();
            if (r.str("reservation_id") != null) ids.add(r.str("reservation_id"));
        });
        assertEquals(1, codes.get(201).get(), codes.toString());
        assertEquals(49, codes.get(200).get(), codes.toString());
        assertEquals(1, ids.size());
        // Seat order doesn't change the request identity.
        assertEquals(200, reserve(show, tok, "same-key", "S4", "S3").code());
        // Different body, same key -> 409 and nothing moves.
        Resp conflict = reserve(show, tok, "same-key", "S5");
        assertEquals(409, conflict.code());
        assertEquals("idempotency_key_conflict", conflict.str("error"));
        assertEquals(2, confirmed(assertAudit(show)));
    }

    @Test
    void sameKeyDifferentBodiesRacing() throws Exception {
        String show = newShow(10, 4);
        String tok = token("racer");
        Map<Integer, AtomicInteger> codes = new ConcurrentHashMap<>();
        parallel(20, i -> codes.computeIfAbsent(reserve(show, tok, "k", "S" + (i % 2 + 1)).code(), k -> new AtomicInteger()).incrementAndGet());
        assertEquals(1, codes.get(201).get(), codes.toString());
        int others = codes.getOrDefault(200, new AtomicInteger()).get() + codes.getOrDefault(409, new AtomicInteger()).get();
        assertEquals(19, others, codes.toString());
        assertEquals(1, confirmed(assertAudit(show)));
    }

    @Test
    void multiSeatNoDeadlockAllOrNothing() throws Exception {
        String show = newShow(30, 4);
        AtomicInteger errors = new AtomicInteger();
        // Overlapping pairs requested in opposite orders: the classic deadlock shape.
        parallel(400, i -> {
            String a = "S" + (i % 10 + 1), b = "S" + ((i + 1) % 10 + 1);
            Resp r = i % 2 == 0 ? reserve(show, token("pair-" + i), "k", b, a) : reserve(show, token("pair-" + i), "k", a, b);
            if (r.code() >= 500) errors.incrementAndGet();
        });
        assertEquals(0, errors.get());
        // All-or-nothing: confirmed seats are always an even number here.
        assertEquals(0, confirmed(assertAudit(show)) % 2);
    }

    @Test
    void cancelOwnershipAndRebook() {
        String show = newShow(5, 4);
        String alice = token("alice"), bob = token("bob");

        // A spoofed body field is ignored; identity is the token's.
        Resp r = call("POST", "/shows/" + show + "/reserve", alice,
                Map.of("seats", List.of("S1"), "idempotency_key", "a1", "user_id", user("bob")));
        assertEquals(201, r.code());
        assertEquals(user("alice"), r.str("user_id"));
        String rid = r.str("reservation_id");

        assertEquals(403, call("POST", "/reservations/" + rid + "/cancel", bob, null).code());
        assertEquals(401, call("POST", "/reservations/" + rid + "/cancel", null, null).code());
        Resp c = call("POST", "/reservations/" + rid + "/cancel", alice, null);
        assertEquals(200, c.code());
        assertEquals("cancelled", c.str("status"));

        // The seat is cleanly re-bookable.
        assertEquals(201, reserve(show, bob, "b1", "S1").code());
        // A repeated cancel of alice's old reservation must not free bob's seat.
        assertEquals(200, call("POST", "/reservations/" + rid + "/cancel", alice, null).code());
        assertEquals(1, call("GET", "/shows/" + show, null, null).body().path("counts").path("confirmed").asInt());
        // Replaying alice's key returns the original (cancelled) reservation, not a new one.
        Resp replay = reserve(show, alice, "a1", "S1");
        assertEquals(200, replay.code());
        assertEquals("cancelled", replay.str("status"));
        assertAudit(show);
    }

    @Test
    void cancelRebookStorm() throws Exception {
        String show = newShow(3, 4);
        AtomicInteger errors = new AtomicInteger();
        parallel(60, i -> {
            String tok = token("churn-" + i);
            for (int j = 0; j < 5; j++) {
                Resp r = reserve(show, tok, "k" + j, "S1");
                if (r.code() >= 500) errors.incrementAndGet();
                if (r.code() == 201 && call("POST", "/reservations/" + r.str("reservation_id") + "/cancel", tok, null).code() != 200) {
                    errors.incrementAndGet();
                }
            }
        });
        assertEquals(0, errors.get());
        assertAudit(show);
    }

    @Test
    void validation() {
        String show = newShow(5, 4);
        String tok = token("v");
        assertEquals(400, call("POST", "/shows", ADMIN, "{\"name\":\"x\",\"seats\":[\"A\"],\"price_paise\":250.5}").code(), "float price");
        assertEquals(400, call("POST", "/shows", ADMIN, "{\"name\":\"x\",\"seats\":[\"A\"],\"price_paise\":\"250\"}").code(), "string price");
        assertEquals(401, call("POST", "/shows", tok, Map.of("name", "x", "seats", List.of("A"), "price_paise", 1)).code(), "non-admin");
        assertEquals(400, reserve(show, tok, "dup", "S1", "S1").code(), "duplicate seats");
        assertEquals(400, reserve(show, tok, "nope", "Z9").code(), "unknown seat");
        assertEquals(404, reserve("00000000-0000-0000-0000-000000000000", tok, "x", "S1").code(), "unknown show");
        assertEquals(404, reserve("not-a-uuid", tok, "x", "S1").code(), "bad show id");
        assertEquals(401, call("POST", "/shows/" + show + "/reserve", "garbage", Map.of("seats", List.of("S1"))).code(), "bad token");
        assertEquals(400, call("POST", "/shows/" + show + "/reserve", tok, "not json").code(), "malformed body");
        assertEquals(404, call("GET", "/nope", null, null).code(), "unknown route");
    }

    @Test
    void readinessAndMetrics() {
        assertEquals(200, call("GET", "/healthz", null, null).code());
        assertEquals("ready", call("GET", "/readyz", null, null).str("status"));
        String show = newShow(3, 4);
        reserve(show, token("m"), "k", "S1");
        HttpResponse<String> m;
        try {
            m = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/metrics")).build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertTrue(m.body().contains("reservations_confirmed_total{show_id=\"" + show + "\"} 1.0"), "confirmed counter");
        assertTrue(m.body().contains("seats_available{show_id=\"" + show + "\"} 2.0"), "available gauge from DB");
    }
}
