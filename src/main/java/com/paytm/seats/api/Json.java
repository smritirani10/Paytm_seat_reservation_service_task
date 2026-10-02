package com.paytm.seats.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.springframework.stereotype.Component;

import jakarta.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.io.InputStream;

/**
 * Bodies are read straight from the servlet input stream and parsed here, so
 * any Content-Type is accepted (curl -d sends form-encoded by default, which
 * Spring would otherwise re-encode as form parameters) and every parse failure
 * is a clean 400 rather than a framework error page.
 */
@Component
public class Json {
    private final ObjectMapper mapper;

    public Json(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public <T> T parse(HttpServletRequest req, Class<T> type, int limit) {
        byte[] body;
        try (InputStream in = req.getInputStream()) {
            body = in.readNBytes(limit + 1);
        } catch (IOException e) {
            throw new ApiException(400, "invalid_json", "could not read request body");
        }
        if (body.length == 0) throw new ApiException(400, "invalid_json", "request body is required");
        if (body.length > limit) throw new ApiException(413, "body_too_large", "request body too large");
        try {
            T v = mapper.readValue(body, type);
            if (v == null) throw new ApiException(400, "invalid_json", "request body must be a JSON object");
            return v;
        } catch (MismatchedInputException e) {
            String field = e.getPath().isEmpty() ? "body" : e.getPath().get(e.getPath().size() - 1).getFieldName();
            throw new ApiException(400, "invalid_json", "field " + field + " has the wrong type (money is integer paise)");
        } catch (IOException e) {
            throw new ApiException(400, "invalid_json", "malformed JSON body");
        }
    }
}
