# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/burst ./cmd/burst

FROM alpine:3.20
# Alpine ships CA certs and busybox wget (used by HEALTHCHECK); no apk needed.
RUN adduser -D -u 10001 app
COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/burst /usr/local/bin/burst
USER app
ENV PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -qO- http://127.0.0.1:${PORT}/readyz >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/server"]
