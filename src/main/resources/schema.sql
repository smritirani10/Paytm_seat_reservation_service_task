-- Schema is idempotent so it can be applied on every boot (cold start safe).

CREATE TABLE IF NOT EXISTS shows (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name           TEXT        NOT NULL,
    price_paise    BIGINT      NOT NULL CHECK (price_paise >= 0),
    per_user_limit INT         NOT NULL CHECK (per_user_limit > 0),
    total_seats    INT         NOT NULL CHECK (total_seats > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    show_id             UUID        NOT NULL REFERENCES shows (id),
    user_id             TEXT        NOT NULL,
    seats               TEXT[]      NOT NULL,
    amount_paise        BIGINT      NOT NULL CHECK (amount_paise >= 0),
    status              TEXT        NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    -- Idempotency record lives in the same row (and therefore the same
    -- transaction) as the reservation it produced: either both exist or neither.
    idempotency_key     TEXT        NOT NULL,
    request_fingerprint TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at        TIMESTAMPTZ,
    -- Keys are scoped per user: one user's key can never collide with another's.
    CONSTRAINT reservations_user_idem_key UNIQUE (user_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS reservations_show_idx ON reservations (show_id);

-- One row per physical seat. The row *is* the seat, so a seat can reference at
-- most one reservation at a time by construction (single reservation_id column,
-- primary key on (show_id, label)).
CREATE TABLE IF NOT EXISTS seats (
    show_id        UUID        NOT NULL REFERENCES shows (id),
    label          TEXT        NOT NULL,
    ordinal        INT         NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'available'
                               CHECK (status IN ('available', 'held', 'confirmed')),
    reservation_id UUID REFERENCES reservations (id),
    user_id        TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (show_id, label),
    -- An available seat has no owner; a held/confirmed seat always has one.
    CONSTRAINT seats_owner_consistent CHECK (
        (status = 'available' AND reservation_id IS NULL AND user_id IS NULL) OR
        (status <> 'available' AND reservation_id IS NOT NULL AND user_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS seats_owner_idx ON seats (show_id, user_id) WHERE status <> 'available';
CREATE INDEX IF NOT EXISTS seats_reservation_idx ON seats (reservation_id) WHERE reservation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS seats_ordinal_idx ON seats (show_id, ordinal);
