package com.paytm.seats.api;

import jakarta.servlet.http.HttpServletRequest;

/** Per-request facts set by handlers and read back by {@link ObservabilityFilter} for the access log. */
final class RequestContext {
    static final String USER = "ctx.user_id";
    static final String OUTCOME = "ctx.outcome";
    static final String REQUEST_ID = "ctx.request_id";

    private RequestContext() {}

    static void user(HttpServletRequest r, String userId) {
        r.setAttribute(USER, userId);
    }

    static void outcome(HttpServletRequest r, String outcome) {
        r.setAttribute(OUTCOME, outcome);
    }
}
