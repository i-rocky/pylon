-- pylon `apps` table (MySQL). pylon READS this table; your control plane writes it.
-- `webhooks` (a JSON array) has NO column DEFAULT: MySQL < 8.0.13 cannot DEFAULT a
-- TEXT column, so supply it on every INSERT ('[]' for an app with no webhooks).
-- Boolean-ish columns use BIGINT (0/1) so the sqlx `Any` driver reads one uniform
-- integer type across SQLite/MySQL/Postgres.
CREATE TABLE IF NOT EXISTS apps (
    id          VARCHAR(255) NOT NULL PRIMARY KEY,
    `key`       VARCHAR(255) NOT NULL UNIQUE,
    secret      VARCHAR(255) NOT NULL,
    name        VARCHAR(255) NOT NULL DEFAULT '',
    capacity    BIGINT NOT NULL DEFAULT 0,
    client_messages_enabled     BIGINT NOT NULL DEFAULT 0,
    subscription_count_enabled  BIGINT NOT NULL DEFAULT 0,
    enabled     BIGINT NOT NULL DEFAULT 1,
    webhooks    TEXT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
