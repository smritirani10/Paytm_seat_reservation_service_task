package com.paytm.seats.api;

import com.paytm.seats.auth.Authenticator;
import com.paytm.seats.metrics.Metrics;
import com.paytm.seats.store.Models.CancelResult;
import com.paytm.seats.store.Models.Reservation;
import com.paytm.seats.store.SeatStore;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

@RestController
public class ReservationController {
    private static final Logger log = LoggerFactory.getLogger(ReservationController.class);

    private final SeatStore store;
    private final Authenticator auth;
    private final Metrics m;

    public ReservationController(SeatStore store, Authenticator auth, Metrics m) {
        this.store = store;
        this.auth = auth;
        this.m = m;
    }

    @GetMapping("/reservations/{id}")
    Reservation get(HttpServletRequest http, @PathVariable String id) throws SQLException {
        return store.getReservation(id, ShowController.requireUser(http, auth));
    }

    /** Owner-only. Repeat cancels are a no-op 200. */
    @PostMapping("/reservations/{id}/cancel")
    Reservation cancel(HttpServletRequest http, @PathVariable String id) throws SQLException {
        String uid = ShowController.requireUser(http, auth);
        CancelResult r = store.cancel(id, uid);
        Reservation res = r.reservation();
        if (r.changed()) {
            m.reservationsCancelled.labelValues(res.showId()).inc();
            m.seatsReleased.labelValues(res.showId()).inc(res.seats().size());
            RequestContext.outcome(http, "cancelled");
            log.atInfo().addKeyValue("show_id", res.showId()).addKeyValue("reservation_id", res.reservationId())
                    .addKeyValue("seats", res.seats()).log("reservation cancelled");
        } else {
            RequestContext.outcome(http, "already_cancelled");
        }
        return res;
    }
}
