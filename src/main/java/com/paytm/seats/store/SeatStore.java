package com.paytm.seats.store;

import com.paytm.seats.store.Models.*;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * The system of record. Every correctness decision (who gets a seat, per-user
 * limits, idempotency) is made inside a single Postgres transaction here;
 * nothing above this layer is trusted to decide.
 */
@Component
public class SeatStore {

    private static final int MAX_TX_ATTEMPTS = 8;
    private static final Pattern UUID_RE = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    /** deadlock, serialization failure, lock timeout, unique violation (retry then replays the winner) */
    private static final Set<String> RETRYABLE = Set.of("40P01", "40001", "55P03", "23505");

    private final DataSource ds;
    private final Map<String, Show> shows = new ConcurrentHashMap<>(); // shows are immutable once created

    public SeatStore(DataSource ds) {
        this.ds = ds;
    }

    // ------------------------------------------------------------------ schema

    /** Applies the idempotent schema under a session advisory lock so several instances booting at once don't race. */
    public void migrate() throws SQLException, IOException {
        String sql;
        try (InputStream in = getClass().getResourceAsStream("/schema.sql")) {
            sql = new String(Objects.requireNonNull(in).readAllBytes(), StandardCharsets.UTF_8);
        }
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(727274)");
            try {
                st.execute(sql);
            } finally {
                st.execute("SELECT pg_advisory_unlock(727274)");
            }
        }
    }

    // ------------------------------------------------------------------ shows

    public ShowState createShow(String name, List<String> labels, long pricePaise, int perUserLimit) throws SQLException {
        Show show = inTx(Connection.TRANSACTION_READ_COMMITTED, c -> {
            Show s;
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO shows (name, price_paise, per_user_limit, total_seats)
                    VALUES (?, ?, ?, ?)
                    RETURNING id::text, name, price_paise, per_user_limit, total_seats, created_at""")) {
                ps.setString(1, name);
                ps.setLong(2, pricePaise);
                ps.setInt(3, perUserLimit);
                ps.setInt(4, labels.size());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    s = readShow(rs);
                }
            }
            Integer[] ordinals = new Integer[labels.size()];
            for (int i = 0; i < ordinals.length; i++) ordinals[i] = i;
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO seats (show_id, label, ordinal)
                    SELECT ?::uuid, l, o FROM unnest(?::text[], ?::int[]) AS t(l, o)""")) {
                ps.setString(1, s.id());
                ps.setArray(2, c.createArrayOf("text", labels.toArray()));
                ps.setArray(3, c.createArrayOf("int4", ordinals));
                ps.executeUpdate();
            }
            return s;
        });
        shows.put(show.id(), show);
        List<SeatState> seats = labels.stream().map(l -> new SeatState(l, Models.SEAT_AVAILABLE)).toList();
        return ShowState.of(show, new Counts(labels.size(), 0, 0, labels.size()), seats);
    }

    public Show getShowMeta(String id) throws SQLException {
        if (!isUuid(id)) throw new ShowNotFoundException();
        Show cached = shows.get(id);
        if (cached != null) return cached;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("""
                SELECT id::text, name, price_paise, per_user_limit, total_seats, created_at
                FROM shows WHERE id = ?::uuid""")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new ShowNotFoundException();
                Show s = readShow(rs);
                shows.put(s.id(), s);
                return s;
            }
        }
    }

    /**
     * Reads every seat in one statement, so counts come from a single MVCC
     * snapshot and available + held + confirmed always equals total_seats.
     */
    public ShowState getShowState(String id) throws SQLException {
        Show show = getShowMeta(id);
        List<SeatState> seats = new ArrayList<>(show.totalSeats());
        int available = 0, held = 0, confirmed = 0;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT label, status FROM seats WHERE show_id = ?::uuid ORDER BY ordinal")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String status = rs.getString(2);
                    seats.add(new SeatState(rs.getString(1), status));
                    switch (status) {
                        case Models.SEAT_AVAILABLE -> available++;
                        case Models.SEAT_HELD -> held++;
                        case Models.SEAT_CONFIRMED -> confirmed++;
                        default -> { }
                    }
                }
            }
        }
        return ShowState.of(show, new Counts(available, held, confirmed, show.totalSeats()), seats);
    }

    // ------------------------------------------------------------------ reserve

    /** Identifies "the same request" for idempotency: same show and same set of seats (order-insensitive). */
    public static String fingerprint(String showId, List<String> seats) {
        List<String> sorted = new ArrayList<>(seats);
        Collections.sort(sorted);
        return showId + "|" + String.join(",", sorted);
    }

    /** All-or-nothing: either every requested seat is confirmed to the caller in one reservation, or none are. */
    public ReserveResult reserve(Show show, String userId, List<String> seats, String idemKey) throws SQLException {
        String fp = fingerprint(show.id(), seats);

        // Fast path: one read-only statement (one snapshot) answering "is this a
        // replay?" and "is any seat already taken?". A reservation and its seat
        // updates commit atomically, so if this snapshot shows a seat taken by our
        // own earlier attempt it also shows that attempt's reservation row: a retry
        // is never misreported as seat_taken. A decline here is truthful (the seat
        // really was taken at the snapshot); the authoritative decision is still
        // the transaction below.
        ReserveResult fast = fastPath(show, userId, seats, idemKey, fp);
        if (fast != null) return fast;

        SQLException last = null;
        for (int attempt = 0; attempt < MAX_TX_ATTEMPTS; attempt++) {
            try {
                return reserveTx(show, userId, seats, idemKey, fp);
            } catch (SQLException e) {
                if (!isRetryable(e)) throw e;
                last = e;
                backoff(attempt);
            }
        }
        throw last;
    }

    private ReserveResult fastPath(Show show, String userId, List<String> seats, String idemKey, String fp) throws SQLException {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("""
                WITH r AS (
                    SELECT id::text AS id, show_id::text AS show_id, user_id, seats, amount_paise, status,
                           request_fingerprint, created_at, cancelled_at
                    FROM reservations WHERE user_id = ? AND idempotency_key = ?
                ), t AS (
                    SELECT count(*)::int AS found,
                           coalesce(array_agg(label ORDER BY label) FILTER (WHERE status <> 'available'), '{}') AS taken
                    FROM seats WHERE show_id = ?::uuid AND label = ANY(?)
                )
                SELECT r.id, r.show_id, r.user_id, r.seats, r.amount_paise, r.status,
                       r.request_fingerprint, r.created_at, r.cancelled_at, t.found, t.taken
                FROM t LEFT JOIN r ON true""")) {
            ps.setString(1, userId);
            ps.setString(2, idemKey);
            ps.setString(3, show.id());
            ps.setArray(4, c.createArrayOf("text", seats.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getString(1) != null) {
                    return replayOrConflict(readReservation(rs), fp, true);
                }
                int found = rs.getInt("found");
                List<String> taken = textArray(rs.getArray("taken"));
                if (found != seats.size()) {
                    return ReserveResult.seats(Outcome.UNKNOWN_SEATS, unknownSeats(c, show.id(), seats), true);
                }
                if (!taken.isEmpty()) {
                    return ReserveResult.seats(Outcome.SEAT_TAKEN, taken, true);
                }
                return null;
            }
        }
    }

    /**
     * The authoritative decision, in one READ COMMITTED transaction:
     * <ol>
     *   <li>Per-user transaction-scoped advisory lock: serialises all reserve
     *       attempts of one user (per-user limit, concurrent same-key retries).
     *       Different users never contend.</li>
     *   <li>Idempotency lookup under that lock.</li>
     *   <li>Per-user limit check (safe: only this user's transactions can raise
     *       this user's holdings, and they are serialised by step 1).</li>
     *   <li>Row-lock the requested seats in a deterministic order
     *       ({@code ORDER BY label FOR UPDATE}) so multi-seat requests can never
     *       deadlock each other.</li>
     *   <li>Insert the reservation (carrying the idempotency key).</li>
     *   <li>Conditional update guarded on {@code status = 'available'}; affected
     *       rows must equal seats requested. Commit.</li>
     * </ol>
     */
    private ReserveResult reserveTx(Show show, String userId, List<String> seats, String idemKey, String fp) throws SQLException {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try {
                ReserveResult r = reserveInTx(c, show, userId, seats, idemKey, fp);
                if (r.outcome() == Outcome.CONFIRMED) c.commit();
                else c.rollback(); // declines change nothing; rollback also releases the locks
                return r;
            } catch (SQLException | RuntimeException e) {
                safeRollback(c);
                throw e;
            }
        }
    }

    private ReserveResult reserveInTx(Connection c, Show show, String userId, List<String> seats, String idemKey, String fp) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended('user:' || ?, 0))")) {
            ps.setString(1, userId);
            ps.execute();
        }

        Reservation existing = reservationByKey(c, userId, idemKey);
        if (existing != null) return replayOrConflict(existing, fp, false);

        int held;
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT count(*) FROM seats
                WHERE show_id = ?::uuid AND user_id = ? AND status <> 'available'""")) {
            ps.setString(1, show.id());
            ps.setString(2, userId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                held = rs.getInt(1);
            }
        }
        if (held + seats.size() > show.perUserLimit()) {
            return new ReserveResult(Outcome.LIMIT_EXCEEDED, null, List.of(), held, show.perUserLimit(), false);
        }

        Array seatArray = c.createArrayOf("text", seats.toArray());
        List<String> taken = new ArrayList<>();
        int locked = 0;
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT label, status FROM seats
                WHERE show_id = ?::uuid AND label = ANY(?)
                ORDER BY label
                FOR UPDATE""")) {
            ps.setString(1, show.id());
            ps.setArray(2, seatArray);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    locked++;
                    if (!Models.SEAT_AVAILABLE.equals(rs.getString(2))) taken.add(rs.getString(1));
                }
            }
        }
        if (locked != seats.size()) {
            return ReserveResult.seats(Outcome.UNKNOWN_SEATS, unknownSeats(c, show.id(), seats), false);
        }
        if (!taken.isEmpty()) {
            return ReserveResult.seats(Outcome.SEAT_TAKEN, taken, false);
        }

        List<String> sorted = new ArrayList<>(seats);
        Collections.sort(sorted);
        long amount = show.pricePaise() * seats.size();
        String resId;
        Instant createdAt;
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO reservations (show_id, user_id, seats, amount_paise, status, idempotency_key, request_fingerprint)
                VALUES (?::uuid, ?, ?, ?, 'confirmed', ?, ?)
                RETURNING id::text, created_at""")) {
            ps.setString(1, show.id());
            ps.setString(2, userId);
            ps.setArray(3, c.createArrayOf("text", sorted.toArray()));
            ps.setLong(4, amount);
            ps.setString(5, idemKey);
            ps.setString(6, fp);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                resId = rs.getString(1);
                createdAt = instant(rs, 2);
            }
        }

        int updated;
        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE seats
                SET status = 'confirmed', reservation_id = ?::uuid, user_id = ?, updated_at = now()
                WHERE show_id = ?::uuid AND label = ANY(?) AND status = 'available'""")) {
            ps.setString(1, resId);
            ps.setString(2, userId);
            ps.setString(3, show.id());
            ps.setArray(4, seatArray);
            updated = ps.executeUpdate();
        }
        if (updated != seats.size()) {
            // Unreachable while we hold the row locks; refuse rather than commit a partial reservation.
            return ReserveResult.seats(Outcome.SEAT_TAKEN, seats, false);
        }
        Reservation r = new Reservation(resId, show.id(), userId, sorted, amount, Models.RES_CONFIRMED, createdAt, null, fp);
        return ReserveResult.res(Outcome.CONFIRMED, r, false);
    }

    private static ReserveResult replayOrConflict(Reservation r, String fp, boolean fast) {
        return ReserveResult.res(r.fingerprint().equals(fp) ? Outcome.REPLAYED : Outcome.IDEMPOTENCY_CONFLICT, r, fast);
    }

    private static List<String> unknownSeats(Connection c, String showId, List<String> seats) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT w FROM unnest(?::text[]) AS w
                WHERE NOT EXISTS (SELECT 1 FROM seats WHERE show_id = ?::uuid AND label = w)
                ORDER BY w""")) {
            ps.setArray(1, c.createArrayOf("text", seats.toArray()));
            ps.setString(2, showId);
            List<String> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
            return out;
        }
    }

    private static final String RESERVATION_COLS = """
            id::text, show_id::text, user_id, seats, amount_paise, status,
            request_fingerprint, created_at, cancelled_at""";

    private static Reservation reservationByKey(Connection c, String userId, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT " + RESERVATION_COLS + " FROM reservations WHERE user_id = ? AND idempotency_key = ?")) {
            ps.setString(1, userId);
            ps.setString(2, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readReservation(rs) : null;
            }
        }
    }

    // ------------------------------------------------------------------ reservations

    public Reservation getReservation(String id, String userId) throws SQLException {
        if (!isUuid(id)) throw new ReservationNotFoundException();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT " + RESERVATION_COLS + " FROM reservations WHERE id = ?::uuid")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new ReservationNotFoundException();
                Reservation r = readReservation(rs);
                if (!r.userId().equals(userId)) throw new NotOwnerException();
                return r;
            }
        }
    }

    /**
     * Releases a reservation's seats. Only the owner may cancel. The seat update
     * is guarded on reservation_id, so a release can only touch seats that still
     * point at <em>this</em> reservation and can never resurrect a seat since
     * sold to someone else. Cancelling twice is a no-op.
     */
    public CancelResult cancel(String id, String userId) throws SQLException {
        if (!isUuid(id)) throw new ReservationNotFoundException();
        SQLException last = null;
        for (int attempt = 0; attempt < MAX_TX_ATTEMPTS; attempt++) {
            try {
                return inTx(Connection.TRANSACTION_READ_COMMITTED, c -> cancelInTx(c, id, userId));
            } catch (SQLException e) {
                if (!isRetryable(e)) throw e;
                last = e;
                backoff(attempt);
            }
        }
        throw last;
    }

    private static CancelResult cancelInTx(Connection c, String id, String userId) throws SQLException {
        Reservation r;
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT " + RESERVATION_COLS + " FROM reservations WHERE id = ?::uuid FOR UPDATE")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new ReservationNotFoundException();
                r = readReservation(rs);
            }
        }
        if (!r.userId().equals(userId)) throw new NotOwnerException();
        if (Models.RES_CANCELLED.equals(r.status())) return new CancelResult(r, false);

        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE seats
                SET status = 'available', reservation_id = NULL, user_id = NULL, updated_at = now()
                WHERE reservation_id = ?::uuid""")) {
            ps.setString(1, id);
            ps.executeUpdate();
        }
        Instant at;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE reservations SET status = 'cancelled', cancelled_at = now() WHERE id = ?::uuid RETURNING cancelled_at")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                at = instant(rs, 1);
            }
        }
        return new CancelResult(r.cancelled(at), true);
    }

    // ------------------------------------------------------------------ audit

    /** Cross-checks the seats table against the reservations table in a single REPEATABLE READ snapshot. */
    public Audit audit(String id) throws SQLException {
        Show show = getShowMeta(id);
        return inTx(Connection.TRANSACTION_REPEATABLE_READ, c -> {
            try (Statement st = c.createStatement()) {
                st.execute("SET TRANSACTION READ ONLY");
            }
            {
                int[] cnt = ints(c, """
                        SELECT count(*) FILTER (WHERE status = 'available'),
                               count(*) FILTER (WHERE status = 'held'),
                               count(*) FILTER (WHERE status = 'confirmed'),
                               count(*)
                        FROM seats WHERE show_id = ?::uuid""", id);
                long[] res = longs(c, """
                        SELECT count(*) FILTER (WHERE status = 'confirmed'),
                               count(*) FILTER (WHERE status = 'cancelled'),
                               coalesce(sum(cardinality(seats)) FILTER (WHERE status = 'confirmed'), 0),
                               coalesce(sum(amount_paise) FILTER (WHERE status = 'confirmed'), 0)
                        FROM reservations WHERE show_id = ?::uuid""", id);
                // Seats marked taken whose reservation is missing, cancelled, someone else's, or doesn't list the seat.
                int orphans = ints(c, """
                        SELECT count(*) FROM seats s
                        LEFT JOIN reservations r ON r.id = s.reservation_id
                        WHERE s.show_id = ?::uuid AND s.status <> 'available'
                          AND (r.id IS NULL OR r.status <> 'confirmed' OR r.user_id <> s.user_id
                               OR NOT (s.label = ANY(r.seats)))""", id)[0];
                // Confirmed reservations that don't own every seat they list.
                int broken = ints(c, """
                        SELECT count(*) FROM reservations r
                        WHERE r.show_id = ?::uuid AND r.status = 'confirmed'
                          AND cardinality(r.seats) <> (SELECT count(*) FROM seats s WHERE s.reservation_id = r.id)""", id)[0];
                int[] perUser;
                try (PreparedStatement ps = c.prepareStatement("""
                        SELECT coalesce(max(n), 0), count(*) FILTER (WHERE n > ?), count(*)
                        FROM (SELECT count(*) AS n FROM seats
                              WHERE show_id = ?::uuid AND status <> 'available' GROUP BY user_id) per_user""")) {
                    ps.setInt(1, show.perUserLimit());
                    ps.setString(2, id);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        perUser = new int[]{rs.getInt(1), rs.getInt(2), rs.getInt(3)};
                    }
                }
                Counts counts = new Counts(cnt[0], cnt[1], cnt[2], cnt[3]);
                long expected = (long) counts.confirmed() * show.pricePaise();
                boolean invariant = counts.available() + counts.held() + counts.confirmed() == show.totalSeats()
                        && counts.totalSeats() == show.totalSeats();
                boolean ok = invariant && orphans == 0 && broken == 0 && perUser[1] == 0
                        && res[2] == counts.confirmed() + counts.held() && res[3] == expected;
                return new Audit(id, ok, counts, invariant, (int) res[0], (int) res[1], (int) res[2],
                        orphans, broken, perUser[0], show.perUserLimit(), perUser[1], res[3], expected, perUser[2]);
            }
        });
    }

    /** Per-status seat counts for the most recent shows; feeds the DB-backed metrics gauges. */
    public Map<String, Counts> seatCountsByShow(int limit) throws SQLException {
        Map<String, Counts> out = new LinkedHashMap<>();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement("""
                SELECT s.id::text, s.total_seats,
                       count(*) FILTER (WHERE st.status = 'available'),
                       count(*) FILTER (WHERE st.status = 'held'),
                       count(*) FILTER (WHERE st.status = 'confirmed')
                FROM (SELECT id, total_seats FROM shows ORDER BY created_at DESC LIMIT ?) s
                JOIN seats st ON st.show_id = s.id
                GROUP BY s.id, s.total_seats""")) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), new Counts(rs.getInt(3), rs.getInt(4), rs.getInt(5), rs.getInt(2)));
                }
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ helpers

    @FunctionalInterface
    private interface TxBody<T> {
        T run(Connection c) throws SQLException;
    }

    private <T> T inTx(int isolation, TxBody<T> body) throws SQLException {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            c.setTransactionIsolation(isolation);
            try {
                T result = body.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                safeRollback(c);
                throw e;
            }
        }
    }

    private static void safeRollback(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // connection is broken; the pool will discard it
        }
    }

    static boolean isRetryable(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null && RETRYABLE.contains(s.getSQLState())) return true;
        }
        return false;
    }

    private static void backoff(int attempt) {
        try {
            Thread.sleep(5L * (attempt + 1));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public static boolean isUuid(String s) {
        return s != null && UUID_RE.matcher(s).matches();
    }

    private static Show readShow(ResultSet rs) throws SQLException {
        return new Show(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getInt(4), rs.getInt(5), instant(rs, 6));
    }

    private static Reservation readReservation(ResultSet rs) throws SQLException {
        return new Reservation(rs.getString(1), rs.getString(2), rs.getString(3), textArray(rs.getArray(4)),
                rs.getLong(5), rs.getString(6), instant(rs, 8), instant(rs, 9), rs.getString(7));
    }

    private static Instant instant(ResultSet rs, int col) throws SQLException {
        OffsetDateTime t = rs.getObject(col, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }

    private static List<String> textArray(Array a) throws SQLException {
        if (a == null) return List.of();
        return List.of((String[]) a.getArray());
    }

    private static int[] ints(Connection c, String sql, String id) throws SQLException {
        long[] l = longs(c, sql, id);
        int[] out = new int[l.length];
        for (int i = 0; i < l.length; i++) out[i] = (int) l[i];
        return out;
    }

    private static long[] longs(Connection c, String sql, String id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                int n = rs.getMetaData().getColumnCount();
                long[] out = new long[n];
                for (int i = 0; i < n; i++) out[i] = rs.getLong(i + 1);
                return out;
            }
        }
    }
}
