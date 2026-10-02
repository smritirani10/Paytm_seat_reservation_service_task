package com.paytm.seats.api;

import com.paytm.seats.config.Readiness;
import com.paytm.seats.metrics.Metrics;
import com.zaxxer.hikari.HikariDataSource;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@RestController
public class HealthController {
    private final Readiness readiness;
    private final HikariDataSource healthDs;
    private final Metrics metrics;
    private final PrometheusTextFormatWriter writer = PrometheusTextFormatWriter.create();

    public HealthController(Readiness readiness, @Qualifier("health") HikariDataSource healthDs, Metrics metrics) {
        this.readiness = readiness;
        this.healthDs = healthDs;
        this.metrics = metrics;
    }

    @GetMapping("/")
    Map<String, Object> index() {
        return Map.of("service", "seat-reservation", "endpoints", List.of(
                "POST /auth/token", "POST /shows (admin)", "GET /shows/{id}", "GET /shows/{id}/audit",
                "POST /shows/{id}/reserve", "GET /reservations/{id}", "POST /reservations/{id}/cancel",
                "GET /healthz", "GET /readyz", "GET /metrics"));
    }

    /** Liveness: the process is up and serving. Never touches the database. */
    @GetMapping("/healthz")
    Map<String, String> healthz() {
        return Map.of("status", "ok");
    }

    /**
     * Readiness fails closed: not ready until migrations ran, and not ready
     * whenever the database doesn't answer within about a second.
     */
    @GetMapping("/readyz")
    ResponseEntity<Map<String, String>> readyz() {
        if (!readiness.isMigrated()) {
            return ResponseEntity.status(503).body(Map.of("status", "starting", "db", "not_migrated"));
        }
        try (Connection c = healthDs.getConnection(); Statement st = c.createStatement()) {
            st.setQueryTimeout(1);
            try (ResultSet rs = st.executeQuery("SELECT 1")) {
                rs.next();
            }
            return ResponseEntity.ok(Map.of("status", "ready", "db", "ok"));
        } catch (Exception e) {
            return ResponseEntity.status(503).body(Map.of("status", "unavailable", "db", "unreachable"));
        }
    }

    @GetMapping("/metrics")
    ResponseEntity<byte[]> metrics() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(64 * 1024);
        writer.write(out, metrics.registry.scrape());
        return ResponseEntity.ok().contentType(MediaType.parseMediaType(PrometheusTextFormatWriter.CONTENT_TYPE)).body(out.toByteArray());
    }
}
