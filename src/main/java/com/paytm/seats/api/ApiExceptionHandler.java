package com.paytm.seats.api;

import com.paytm.seats.store.Models.NotOwnerException;
import com.paytm.seats.store.Models.ReservationNotFoundException;
import com.paytm.seats.store.Models.ShowNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.sql.SQLException;
import java.util.Map;

/**
 * Maps exceptions to JSON. Domain declines are 4xx. The only 5xx paths are a
 * genuinely unreachable/failing database (503, retry-safe with the same
 * idempotency key) and an unexpected bug (500); both are logged at error.
 */
@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<Map<String, Object>> api(ApiException e) {
        return ResponseEntity.status(e.status).body(e.body);
    }

    @ExceptionHandler(ShowNotFoundException.class)
    ResponseEntity<Map<String, Object>> show() {
        return ResponseEntity.status(404).body(ApiException.body("show_not_found", "no such show"));
    }

    @ExceptionHandler(ReservationNotFoundException.class)
    ResponseEntity<Map<String, Object>> reservation() {
        return ResponseEntity.status(404).body(ApiException.body("reservation_not_found", "no such reservation"));
    }

    @ExceptionHandler(NotOwnerException.class)
    ResponseEntity<Map<String, Object>> owner(HttpServletRequest r) {
        RequestContext.outcome(r, "forbidden_not_owner");
        return ResponseEntity.status(403).body(ApiException.body("forbidden", "only the reservation owner may do this"));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<Map<String, Object>> notFound() {
        return ResponseEntity.status(404).body(ApiException.body("not_found", "no such route"));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    ResponseEntity<Map<String, Object>> method() {
        return ResponseEntity.status(405).body(ApiException.body("method_not_allowed", "method not allowed on this route"));
    }

    @ExceptionHandler({SQLException.class, DataAccessException.class})
    ResponseEntity<Map<String, Object>> db(Exception e) {
        log.atError().addKeyValue("err", String.valueOf(e.getMessage())).log("internal error");
        return ResponseEntity.status(503).header("Retry-After", "1").body(ApiException.body("unavailable",
                "temporarily unable to process request; safe to retry with the same idempotency key"));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Map<String, Object>> other(Exception e) {
        log.atError().setCause(e).log("unexpected error");
        return ResponseEntity.status(500).body(ApiException.body("internal_error", "unexpected error"));
    }
}
