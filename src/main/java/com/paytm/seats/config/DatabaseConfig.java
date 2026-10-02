package com.paytm.seats.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Two pools against the same database:
 * <ul>
 *   <li>the main pool, which never connects eagerly (initializationFailTimeout=-1),
 *       so the app boots and serves /healthz even while the DB is still starting;</li>
 *   <li>a tiny health pool with a 1s connection timeout, so /readyz answers fast
 *       and fails closed instead of queueing behind the main pool.</li>
 * </ul>
 */
@Configuration
public class DatabaseConfig {

    @Value("${DATABASE_URL:postgres://postgres:postgres@localhost:5432/seats?sslmode=disable}")
    private String databaseUrl;

    @Value("${DB_MAX_CONNS:40}")
    private int maxConns;

    @Bean
    @Primary
    public HikariDataSource dataSource() {
        HikariConfig c = base("seat-pool");
        c.setMaximumPoolSize(maxConns);
        c.setMinimumIdle(2);
        c.setConnectionTimeout(30_000);
        c.setIdleTimeout(300_000);
        // Safety net: no statement may wait on a lock forever.
        c.setConnectionInitSql("SET lock_timeout = '20s'; SET statement_timeout = '25s'");
        return new HikariDataSource(c);
    }

    @Bean
    @Qualifier("health")
    public HikariDataSource healthDataSource() {
        HikariConfig c = base("health-pool");
        c.setMaximumPoolSize(2);
        c.setMinimumIdle(0);
        c.setConnectionTimeout(1_000);
        c.setValidationTimeout(1_000);
        return new HikariDataSource(c);
    }

    private HikariConfig base(String name) {
        HikariConfig c = new HikariConfig();
        c.setPoolName(name);
        c.setInitializationFailTimeout(-1); // never fail boot because the DB isn't up yet
        JdbcUrl u = toJdbc(databaseUrl);
        c.setJdbcUrl(u.url());
        if (u.user() != null) c.setUsername(u.user());
        if (u.password() != null) c.setPassword(u.password());
        c.addDataSourceProperty("ApplicationName", "seat-reservation");
        c.addDataSourceProperty("connectTimeout", "5");
        return c;
    }

    record JdbcUrl(String url, String user, String password) {}

    /** Accepts both jdbc:postgresql://... and the postgres://user:pass@host/db form PaaS providers hand out. */
    static JdbcUrl toJdbc(String raw) {
        if (raw.startsWith("jdbc:")) return new JdbcUrl(raw, null, null);
        URI uri = URI.create(raw);
        String user = null, pass = null;
        if (uri.getRawUserInfo() != null) {
            String[] parts = uri.getRawUserInfo().split(":", 2);
            user = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            if (parts.length > 1) pass = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
        }
        int port = uri.getPort() == -1 ? 5432 : uri.getPort();
        String url = "jdbc:postgresql://" + uri.getHost() + ":" + port + uri.getRawPath()
                + (uri.getRawQuery() != null ? "?" + uri.getRawQuery() : "");
        return new JdbcUrl(url, user, pass);
    }
}
