package com.paytm.seats.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Issues and verifies HS256 JWTs. The user id used by every handler comes from
 * the verified token's subject, never from a request body.
 */
@Component
public class Authenticator {
    private static final Logger log = LoggerFactory.getLogger(Authenticator.class);
    private static final Pattern USER_ID = Pattern.compile("^[A-Za-z0-9_.:@-]{1,128}$");
    private static final String ISSUER = "seat-reservation";
    private static final Duration TTL = Duration.ofHours(24);

    private final Algorithm alg;
    private final JWTVerifier verifier;
    private final byte[] adminToken;

    public Authenticator(@Value("${JWT_SECRET:}") String secret, @Value("${ADMIN_TOKEN:}") String admin) {
        if (secret.isBlank()) {
            secret = "dev-insecure-secret-change-me";
            log.warn("JWT_SECRET not set; using an insecure development secret");
        }
        if (admin.isBlank()) {
            admin = "dev-admin-token";
            log.warn("ADMIN_TOKEN not set; using the development admin token");
        }
        this.alg = Algorithm.HMAC256(secret);
        this.verifier = JWT.require(alg).withIssuer(ISSUER).build();
        this.adminToken = admin.getBytes(StandardCharsets.UTF_8);
    }

    public static boolean validUserId(String id) {
        return id != null && USER_ID.matcher(id).matches();
    }

    public record Issued(String token, Instant expiresAt) {}

    public Issued issue(String userId) {
        Instant now = Instant.now();
        Instant exp = now.plus(TTL);
        String tok = JWT.create().withIssuer(ISSUER).withSubject(userId).withIssuedAt(now).withExpiresAt(exp).sign(alg);
        return new Issued(tok, exp);
    }

    /** @return the authenticated user id, or null if the token is missing/invalid/expired. */
    public String userFrom(HttpServletRequest req) {
        String tok = bearer(req);
        if (tok == null) return null;
        try {
            DecodedJWT jwt = verifier.verify(tok);
            if (jwt.getExpiresAt() == null || !validUserId(jwt.getSubject())) return null;
            return jwt.getSubject();
        } catch (JWTVerificationException e) {
            return null;
        }
    }

    public boolean hasToken(HttpServletRequest req) {
        return bearer(req) != null;
    }

    /** Static admin token, constant-time compare. */
    public boolean isAdmin(HttpServletRequest req) {
        String tok = bearer(req);
        if (tok == null) tok = req.getHeader("X-Admin-Token");
        return tok != null && !tok.isEmpty()
                && MessageDigest.isEqual(tok.getBytes(StandardCharsets.UTF_8), adminToken);
    }

    private static String bearer(HttpServletRequest req) {
        String h = req.getHeader("Authorization");
        if (h != null && h.length() > 7 && h.regionMatches(true, 0, "bearer ", 0, 7)) {
            String t = h.substring(7).trim();
            return t.isEmpty() ? null : t;
        }
        return null;
    }
}
