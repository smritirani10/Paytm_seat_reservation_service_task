package com.paytm.seats.store;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

/** Value types returned by {@link SeatStore}. JSON is snake_case (see application.yml). */
public final class Models {
    private Models() {}

    public static final String SEAT_AVAILABLE = "available";
    public static final String SEAT_HELD = "held";
    public static final String SEAT_CONFIRMED = "confirmed";
    public static final String RES_CONFIRMED = "confirmed";
    public static final String RES_CANCELLED = "cancelled";

    public record Show(String id, String name, long pricePaise, int perUserLimit, int totalSeats, Instant createdAt) {}

    public record SeatState(String label, String status) {}

    public record Counts(int available, int held, int confirmed, int totalSeats) {}

    public record ShowState(String id, String name, long pricePaise, int perUserLimit, int totalSeats,
                            Instant createdAt, Counts counts, List<SeatState> seats) {
        static ShowState of(Show s, Counts c, List<SeatState> seats) {
            return new ShowState(s.id(), s.name(), s.pricePaise(), s.perUserLimit(), s.totalSeats(), s.createdAt(), c, seats);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Reservation(String reservationId, String showId, String userId, List<String> seats,
                              long amountPaise, String status, Instant createdAt, Instant cancelledAt,
                              @JsonIgnore String fingerprint) {
        Reservation cancelled(Instant at) {
            return new Reservation(reservationId, showId, userId, seats, amountPaise, RES_CANCELLED, createdAt, at, fingerprint);
        }
    }

    /** Domain result of a reserve attempt. Declines are outcomes, not exceptions. */
    public enum Outcome { CONFIRMED, REPLAYED, SEAT_TAKEN, LIMIT_EXCEEDED, IDEMPOTENCY_CONFLICT, UNKNOWN_SEATS }

    public record ReserveResult(Outcome outcome, Reservation reservation, List<String> seats,
                                int held, int limit, boolean fastPath) {
        static ReserveResult of(Outcome o, boolean fast) { return new ReserveResult(o, null, List.of(), 0, 0, fast); }
        static ReserveResult seats(Outcome o, List<String> seats, boolean fast) { return new ReserveResult(o, null, seats, 0, 0, fast); }
        static ReserveResult res(Outcome o, Reservation r, boolean fast) { return new ReserveResult(o, r, List.of(), 0, 0, fast); }
    }

    public record CancelResult(Reservation reservation, boolean changed) {}

    public record Audit(String showId, boolean ok, Counts counts, boolean invariantHolds,
                        int confirmedReservations, int cancelledReservations, int seatsInConfirmedReservations,
                        int orphanSeats, int brokenReservations, int maxSeatsPerUser, int perUserLimit,
                        int usersOverLimit, long confirmedRevenuePaise, long expectedRevenuePaise,
                        int distinctUsersHoldingSeats) {}

    public static class ShowNotFoundException extends RuntimeException {}

    public static class ReservationNotFoundException extends RuntimeException {}

    public static class NotOwnerException extends RuntimeException {}
}
