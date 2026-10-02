package com.paytm.seats.api;

import com.paytm.seats.auth.Authenticator;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Test token mint standing in for a real identity provider. */
@RestController
public class AuthController {
    record TokenRequest(String userId) {}

    private final Authenticator auth;
    private final Json json;

    public AuthController(Authenticator auth, Json json) {
        this.auth = auth;
        this.json = json;
    }

    @PostMapping("/auth/token")
    Map<String, Object> issue(HttpServletRequest http) {
        TokenRequest req = json.parse(http, TokenRequest.class, 4 << 10);
        if (!Authenticator.validUserId(req.userId())) {
            throw new ApiException(400, "invalid_user_id", "user_id must match [A-Za-z0-9_.:@-]{1,128}");
        }
        Authenticator.Issued t = auth.issue(req.userId());
        return Map.of("token", t.token(), "user_id", req.userId(), "expires_at", t.expiresAt());
    }
}
