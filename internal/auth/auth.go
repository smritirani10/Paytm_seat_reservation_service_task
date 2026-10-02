// Package auth issues and verifies HS256 JWTs. The user id used by every
// handler comes from the verified token's subject, never from a request body.
package auth

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrNoToken      = errors.New("missing bearer token")
	ErrInvalidToken = errors.New("invalid or expired token")
	userIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_.:@-]{1,128}$`)
)

type Authenticator struct {
	secret     []byte
	adminToken string
	ttl        time.Duration
}

func New(secret, adminToken string, ttl time.Duration) *Authenticator {
	return &Authenticator{secret: []byte(secret), adminToken: adminToken, ttl: ttl}
}

func ValidUserID(id string) bool { return userIDPattern.MatchString(id) }

func (a *Authenticator) Issue(userID string) (string, time.Time, error) {
	exp := time.Now().Add(a.ttl)
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(exp),
		Issuer:    "seat-reservation",
	})
	s, err := tok.SignedString(a.secret)
	return s, exp, err
}

func (a *Authenticator) Verify(token string) (string, error) {
	claims := &jwt.RegisteredClaims{}
	_, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		return a.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer("seat-reservation"), jwt.WithExpirationRequired())
	if err != nil || !ValidUserID(claims.Subject) {
		return "", ErrInvalidToken
	}
	return claims.Subject, nil
}

// UserFromRequest returns the authenticated user id from the Authorization header.
func (a *Authenticator) UserFromRequest(r *http.Request) (string, error) {
	tok, ok := bearer(r)
	if !ok {
		return "", ErrNoToken
	}
	return a.Verify(tok)
}

// IsAdmin checks the static admin token (constant-time compare).
func (a *Authenticator) IsAdmin(r *http.Request) bool {
	tok, ok := bearer(r)
	if !ok {
		tok = r.Header.Get("X-Admin-Token")
	}
	return a.adminToken != "" && tok != "" &&
		subtle.ConstantTimeCompare([]byte(tok), []byte(a.adminToken)) == 1
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:]), true
	}
	return "", false
}
