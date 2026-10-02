package com.paytm.seats.metrics;

import com.paytm.seats.store.Models.Counts;
import com.paytm.seats.store.SeatStore;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.ToIntFunction;

/**
 * Prometheus metrics. Counters are incremented by the API after each domain
 * outcome; seat gauges are read straight from the database at scrape time so
 * they always reconcile with GET /shows/{id}.
 */
@Component
public class Metrics {
    public static final String SEAT_TAKEN = "seat_taken";
    public static final String PER_USER_LIMIT = "per_user_limit";
    public static final String IDEMPOTENT_REPLAY = "idempotent_replay";
    public static final String IDEMPOTENCY_CONFLICT = "idempotency_conflict";
    public static final String UNKNOWN_SEAT = "unknown_seat";

    private static final Logger log = LoggerFactory.getLogger(Metrics.class);
    private static final int SHOWS_TRACKED = 25;

    public final PrometheusRegistry registry = new PrometheusRegistry();

    public final Counter reservationsConfirmed = Counter.builder().name("reservations_confirmed_total")
            .help("Reservations confirmed (201).").labelNames("show_id").register(registry);
    public final Counter reservationsDeclined = Counter.builder().name("reservations_declined_total")
            .help("Reserve requests that did not create a new reservation, by reason.").labelNames("show_id", "reason").register(registry);
    public final Counter seatsConfirmed = Counter.builder().name("seats_confirmed_total")
            .help("Seats moved available -> confirmed.").labelNames("show_id").register(registry);
    public final Counter reservationsCancelled = Counter.builder().name("reservations_cancelled_total")
            .help("Reservations cancelled by their owner.").labelNames("show_id").register(registry);
    public final Counter seatsReleased = Counter.builder().name("seats_released_total")
            .help("Seats moved confirmed -> available by a cancel.").labelNames("show_id").register(registry);
    public final Counter fastPath = Counter.builder().name("reserve_fast_path_total")
            .help("Reserve outcomes decided by the lock-free pre-check, by outcome.").labelNames("outcome").register(registry);
    public final Counter reserveErrors = Counter.builder().name("reserve_internal_errors_total")
            .help("Reserve attempts that failed with an infrastructure error (page on this).").register(registry);
    public final Counter httpRequests = Counter.builder().name("http_requests_total")
            .help("HTTP requests by route and status code.").labelNames("method", "route", "code").register(registry);
    public final Histogram httpDuration = Histogram.builder().name("http_request_duration_seconds")
            .help("HTTP request latency by route.").labelNames("method", "route").classicOnly()
            .classicUpperBounds(.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30).register(registry);
    public final Gauge inFlight = Gauge.builder().name("http_requests_in_flight")
            .help("HTTP requests currently being served.").register(registry);

    public Metrics(SeatStore store, HikariDataSource ds) {
        JvmMetrics.builder().register(registry);
        registry.register(new SeatCollector(store));
        poolGauge(ds, "db_pool_acquired_conns", "Connections currently checked out.", HikariPoolMXBean::getActiveConnections);
        poolGauge(ds, "db_pool_idle_conns", "Idle connections.", HikariPoolMXBean::getIdleConnections);
        poolGauge(ds, "db_pool_total_conns", "Open connections.", HikariPoolMXBean::getTotalConnections);
        poolGauge(ds, "db_pool_pending_acquires", "Threads waiting for a connection.", HikariPoolMXBean::getThreadsAwaitingConnection);
        GaugeWithCallback.builder().name("db_pool_max_conns").help("Pool size limit.")
                .callback(cb -> cb.call(ds.getMaximumPoolSize())).register(registry);
    }

    private void poolGauge(HikariDataSource ds, String name, String help, ToIntFunction<HikariPoolMXBean> f) {
        GaugeWithCallback.builder().name(name).help(help).callback(cb -> {
            HikariPoolMXBean mx = ds.getHikariPoolMXBean();
            cb.call(mx == null ? 0 : f.applyAsInt(mx));
        }).register(registry);
    }

    public void declined(String showId, String reason) {
        reservationsDeclined.labelValues(showId, reason).inc();
    }

    /** seats{show_id,status}, seats_available, show_total_seats, seat_invariant_ok - all from the database, cached 500ms. */
    private static final class SeatCollector implements MultiCollector {
        private final SeatStore store;
        private long at;
        private Map<String, Counts> cached = Map.of();

        SeatCollector(SeatStore store) {
            this.store = store;
        }

        @Override
        public synchronized MetricSnapshots collect() {
            if (System.currentTimeMillis() - at > 500) {
                try {
                    cached = store.seatCountsByShow(SHOWS_TRACKED);
                    at = System.currentTimeMillis();
                } catch (Exception e) {
                    log.atWarn().addKeyValue("err", String.valueOf(e.getMessage())).log("metrics: seat counts query failed");
                }
            }
            GaugeSnapshot.Builder seats = GaugeSnapshot.builder().name("seats").help("Seats per show by status (read from the database).");
            GaugeSnapshot.Builder available = GaugeSnapshot.builder().name("seats_available").help("Available seats per show (read from the database).");
            GaugeSnapshot.Builder total = GaugeSnapshot.builder().name("show_total_seats").help("Total seats per show.");
            GaugeSnapshot.Builder invariant = GaugeSnapshot.builder().name("seat_invariant_ok")
                    .help("1 if available+held+confirmed == total_seats for the show, else 0.");
            cached.forEach((id, n) -> {
                seats.dataPoint(point(n.available(), "show_id", id, "status", "available"));
                seats.dataPoint(point(n.held(), "show_id", id, "status", "held"));
                seats.dataPoint(point(n.confirmed(), "show_id", id, "status", "confirmed"));
                available.dataPoint(point(n.available(), "show_id", id));
                total.dataPoint(point(n.totalSeats(), "show_id", id));
                boolean ok = n.available() + n.held() + n.confirmed() == n.totalSeats();
                invariant.dataPoint(point(ok ? 1 : 0, "show_id", id));
            });
            return MetricSnapshots.of(seats.build(), available.build(), total.build(), invariant.build());
        }

        private static GaugeDataPointSnapshot point(double v, String... labels) {
            return GaugeDataPointSnapshot.builder().value(v).labels(Labels.of(labels)).build();
        }
    }
}
