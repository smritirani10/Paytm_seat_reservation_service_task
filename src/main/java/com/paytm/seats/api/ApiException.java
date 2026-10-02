package com.paytm.seats.api;

import java.util.LinkedHashMap;
import java.util.Map;

/** A clean 4xx domain response: carries the status and JSON body to send. */
public class ApiException extends RuntimeException {
    final int status;
    final Map<String, Object> body;

    public ApiException(int status, String error, String message) {
        super(error, null, false, false);
        this.status = status;
        this.body = new LinkedHashMap<>();
        body.put("error", error);
        body.put("message", message);
    }

    static Map<String, Object> body(String error, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", error);
        m.put("message", message);
        return m;
    }
}
