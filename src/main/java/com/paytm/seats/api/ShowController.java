package com.paytm.seats.api;

import com.paytm.seats.auth.Authenticator;
import com.paytm.seats.metrics.Metrics;
import com.paytm.seats.store.Models.*;
import com.paytm.seats.store.SeatStore;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Shows and reservations. Authenticates, validates, maps store outcomes to HTTP; makes no seat decisions itself. */
@RestController
public class ShowController {
    private static final Logger log = LoggerFactory.getLogger(ShowController.class);
    static final int DEFAULT_PER_USER_LIMIT = 4;
    static final int MAX_SEATS_PER_SHOW = 100_000;
    static final int MAX_SEATS_PER_REQUEST = 20;
    static final int MAX_LABEL_LEN = 32;
    static final int MAX_IDEM_KEY_LEN = 200;

    record CreateShowRequest(String name, List<String> seats, Long pricePaise, Integer perUserLimit) {}

    /** Unknown fields (e.g. a spoofed "user_id") are ignored: identity only ever comes from the token. */
    record ReserveRequest(List<String> seats, String idempotencyKey) {}

    private final SeatStore store;
    private final Authenticator auth;
    private final Metrics m;
    private final Json json;

    public ShowController(SeatStore store, Authenticator auth, Metrics m, Json json) {
        this.store = store;
        this.auth = auth;
        this.m = m;
        this.json = json;
    }

    // ------------------------------------------------------------------ shows

    @PostMapping("/shows")
    ResponseEntity<ShowState> create(HttpServletRequest http) throws SQLException {
        if (!auth.isAdmin(http)) throw new ApiException(401, "unauthorized", "admin token required");
        CreateShowRequest req = json.parse(http, CreateShowRequest.class, 8 << 20);
        String name = req.name() == null ? "" : req.name().trim();
        if (name.isEmpty() || name.length() > 200) {
            throw new ApiException(400, "invalid_name", "name is required (max 200 chars)");
        }
        if (req.seats() == null || req.seats().isEmpty() || req.seats().size() > MAX_SEATS_PER_SHOW) {
            throw new ApiException(400, "invalid_seats", "seats must be a non-empty list (max 100000)");
        }
        if (req.pricePaise() == null || req.pricePaise() < 0) {
            throw new ApiException(400, "invalid_price", "price_paise must be a non-negative integer");
        }
        int limit = DEFAULT_PER_USER_LIMIT;
        if (req.perUserLimit() != null) {
            if (req.perUserLimit() < 1 || req.perUserLimit() > 1000) {
                throw new ApiException(400, "invalid_per_user_limit", "per_user_limit must be 1..1000");
            }
            limit = req.perUserLimit();
        }
        validateLabels(req.seats(), "duplicate seat label: ");
        ShowState show = store.createShow(name, req.seats(), req.pricePaise(), limit);
        log.atInfo().addKeyValue("show_id", show.id()).addKeyValue("seats", show.totalSeats())
                .addKeyValue("price_paise", show.pricePaise()).addKeyValue("per_user_limit", show.perUserLimit())
                .log("show created");
        return ResponseEntity.status(201).body(show);
    }

    @GetMapping("/shows/{id}")
    ShowState get(@PathVariable String id) throws SQLException {
        return store.getShowState(id);
    }

    @GetMapping("/shows/{id}/audit")
    Audit audit(@PathVariable String id) throws SQLException {
        Audit a = store.audit(id);
        if (!a.ok()) log.atError().addKeyValue("show_id", id).addKeyValue("audit", a.toString()).log("AUDIT FAILED");
        return a;
    }

    // ------------------------------------------------------------------ reserve

    @PostMapping("/shows/{id}/reserve")
    ResponseEntity<?> reserve(HttpServletRequest http, @PathVariable String id) throws SQLException {
        String uid = requireUser(http, auth);
        ReserveRequest req = json.parse(http, ReserveRequest.class, 64 << 10);

        String header = http.getHeader("Idempotency-Key");
        String key = header == null ? "" : header.trim();
        String bodyKey = req.idempotencyKey() == null ? "" : req.idempotencyKey();
        if (!key.isEmpty() && !bodyKey.isEmpty() && !key.equals(bodyKey)) {
            throw new ApiException(400, "invalid_idempotency_key", "Idempotency-Key header and body idempotency_key differ");
        }
        if (key.isEmpty()) key = bodyKey;
        if (key.length() > MAX_IDEM_KEY_LEN) {
            throw new ApiException(400, "invalid_idempotency_key", "idempotency key too long (max 200)");
        }
        if (key.isEmpty()) key = "auto:" + ObservabilityFilter.newId() + ObservabilityFilter.newId(); // not retry-safe
        if (req.seats() == null || req.seats().isEmpty() || req.seats().size() > MAX_SEATS_PER_REQUEST) {
            throw new ApiException(400, "invalid_seats", "seats must be a non-empty list (max 20)");
        }
        validateLabels(req.seats(), "duplicate seat in request: ");

        Show show = store.getShowMeta(id);
        ReserveResult r;
        try {
            r = store.reserve(show, uid, req.seats(), key);
        } catch (SQLException e) {
            m.reserveErrors.inc();
            throw e;
        }
        String outcome = outcomeName(r.outcome());
        if (r.fastPath()) m.fastPath.labelValues(outcome).inc();
        RequestContext.outcome(http, outcome);

        Map<String, Object> decline = new LinkedHashMap<>();
        switch (r.outcome()) {
            case CONFIRMED -> {
                m.reservationsConfirmed.labelValues(show.id()).inc();
                m.seatsConfirmed.labelValues(show.id()).inc(r.reservation().seats().size());
                log.atInfo().addKeyValue("show_id", show.id()).addKeyValue("reservation_id", r.reservation().reservationId())
                        .addKeyValue("seats", r.reservation().seats()).addKeyValue("user_id", uid).log("reservation confirmed");
                return ResponseEntity.status(201).body(r.reservation());
            }
            case REPLAYED -> {
                m.declined(show.id(), Metrics.IDEMPOTENT_REPLAY);
                return ResponseEntity.ok().header("Idempotent-Replayed", "true").body(r.reservation());
            }
            case IDEMPOTENCY_CONFLICT -> {
                m.declined(show.id(), Metrics.IDEMPOTENCY_CONFLICT);
                decline.put("error", "idempotency_key_conflict");
                decline.put("message", "this idempotency key was already used with a different request body");
                decline.put("original_reservation_id", r.reservation().reservationId());
                return ResponseEntity.status(409).body(decline);
            }
            case SEAT_TAKEN -> {
                m.declined(show.id(), Metrics.SEAT_TAKEN);
                decline.put("error", "seat_taken");
                decline.put("message", "one or more requested seats are already taken; nothing was reserved");
                decline.put("seats", r.seats());
                return ResponseEntity.status(409).body(decline);
            }
            case LIMIT_EXCEEDED -> {
                m.declined(show.id(), Metrics.PER_USER_LIMIT);
                decline.put("error", "per_user_limit_exceeded");
                decline.put("message", "reservation would exceed the per-user seat limit for this show");
                decline.put("per_user_limit", r.limit());
                decline.put("already_held", r.held());
                decline.put("requested", req.seats().size());
                return ResponseEntity.status(409).body(decline);
            }
            case UNKNOWN_SEATS -> {
                m.declined(show.id(), Metrics.UNKNOWN_SEAT);
                decline.put("error", "unknown_seats");
                decline.put("message", "requested seats do not exist in this show");
                decline.put("seats", r.seats());
                return ResponseEntity.status(400).body(decline);
            }
        }
        throw new IllegalStateException("unhandled outcome " + r.outcome());
    }

    static String outcomeName(Outcome o) {
        return switch (o) {
            case CONFIRMED -> "confirmed";
            case REPLAYED -> Metrics.IDEMPOTENT_REPLAY;
            case IDEMPOTENCY_CONFLICT -> Metrics.IDEMPOTENCY_CONFLICT;
            case SEAT_TAKEN -> Metrics.SEAT_TAKEN;
            case LIMIT_EXCEEDED -> Metrics.PER_USER_LIMIT;
            case UNKNOWN_SEATS -> Metrics.UNKNOWN_SEAT;
        };
    }

    // ------------------------------------------------------------------ helpers

    static String requireUser(HttpServletRequest http, Authenticator auth) {
        String uid = auth.userFrom(http);
        if (uid == null) {
            throw new ApiException(401, "unauthorized", auth.hasToken(http) ? "invalid or expired token" : "missing bearer token");
        }
        RequestContext.user(http, uid);
        return uid;
    }

    private static void validateLabels(List<String> labels, String dupMsg) {
        Set<String> seen = new HashSet<>(labels.size() * 2);
        for (String l : labels) {
            if (!validLabel(l)) throw new ApiException(400, "invalid_seats", "invalid seat label: " + truncate(l));
            if (!seen.add(l)) throw new ApiException(400, "invalid_seats", dupMsg + l);
        }
    }

    static boolean validLabel(String l) {
        if (l == null || l.isEmpty() || l.length() > MAX_LABEL_LEN) return false;
        for (int i = 0; i < l.length(); i++) {
            char c = l.charAt(i);
            if (c < 0x21 || c > 0x7e) return false;
        }
        return true;
    }

    private static String truncate(String s) {
        if (s == null) return "null";
        return s.length() > 40 ? s.substring(0, 40) + "..." : s;
    }
}
