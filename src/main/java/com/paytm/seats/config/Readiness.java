package com.paytm.seats.config;

import com.paytm.seats.store.SeatStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cold-start handling: once the HTTP server is up, migrate in the background
 * with exponential backoff until the database is reachable. Until then
 * /readyz reports not-ready.
 */
@Component
public class Readiness {
    private static final Logger log = LoggerFactory.getLogger(Readiness.class);

    private final SeatStore store;
    private final AtomicBoolean migrated = new AtomicBoolean(false);
    private volatile boolean shuttingDown;

    public Readiness(SeatStore store) {
        this.store = store;
    }

    public boolean isMigrated() {
        return migrated.get() && !shuttingDown;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Thread.ofVirtual().name("migrator").start(this::migrateUntilReady);
    }

    @EventListener(ContextClosedEvent.class)
    public void stop() {
        shuttingDown = true;
    }

    private void migrateUntilReady() {
        long backoff = 500;
        for (int attempt = 1; !shuttingDown; attempt++) {
            try {
                store.migrate();
                migrated.set(true);
                log.atInfo().addKeyValue("attempt", attempt).log("database ready");
                return;
            } catch (Exception e) {
                log.atWarn().addKeyValue("attempt", attempt).addKeyValue("err", String.valueOf(e.getMessage()))
                        .addKeyValue("backoff_ms", backoff).log("database not ready, retrying");
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                return;
            }
            backoff = Math.min(backoff * 2, 10_000);
        }
    }
}
