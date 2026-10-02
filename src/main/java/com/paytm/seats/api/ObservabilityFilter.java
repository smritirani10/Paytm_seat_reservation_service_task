package com.paytm.seats.api;

import com.paytm.seats.metrics.Metrics;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerMapping;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Assigns a correlation id (X-Request-ID, echoed back), records request
 * metrics and writes one structured access-log line per request.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ObservabilityFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger("access");
    private static final Pattern REQUEST_ID = Pattern.compile("^[A-Za-z0-9._-]{1,64}$");
    private static final Set<String> QUIET = Set.of("GET /metrics", "GET /healthz", "GET /readyz");
    private static final SecureRandom RNG = new SecureRandom();

    private final Metrics m;

    public ObservabilityFilter(Metrics m) {
        this.m = m;
    }

    static String newId() {
        byte[] b = new byte[8];
        RNG.nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        long start = System.nanoTime();
        String id = req.getHeader("X-Request-ID");
        if (id == null || !REQUEST_ID.matcher(id).matches()) id = newId();
        req.setAttribute(RequestContext.REQUEST_ID, id);
        res.setHeader("X-Request-ID", id);
        MDC.put("request_id", id);
        m.inFlight.inc();
        try {
            chain.doFilter(req, res);
        } finally {
            m.inFlight.dec();
            Object pattern = req.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            String route = pattern == null || "/**".equals(pattern) ? "unmatched" : req.getMethod() + " " + pattern;
            int status = res.getStatus();
            double secs = (System.nanoTime() - start) / 1e9;
            m.httpRequests.labelValues(req.getMethod(), route, Integer.toString(status)).inc();
            m.httpDuration.labelValues(req.getMethod(), route).observe(secs);
            if (!QUIET.contains(route)) {
                LoggingEventBuilder e = status >= 500 ? log.atError() : log.atInfo();
                e = e.addKeyValue("method", req.getMethod()).addKeyValue("route", route)
                        .addKeyValue("path", req.getRequestURI()).addKeyValue("status", status)
                        .addKeyValue("duration_ms", Math.round(secs * 1e6) / 1e3);
                Object user = req.getAttribute(RequestContext.USER);
                if (user != null) e = e.addKeyValue("user_id", user);
                Object outcome = req.getAttribute(RequestContext.OUTCOME);
                if (outcome != null) e = e.addKeyValue("outcome", outcome);
                e.log("request");
            }
            MDC.remove("request_id");
        }
    }
}
