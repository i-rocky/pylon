-- pylon `apps` table (SQLite — single-node/dev/edge). pylon READS this table; your
-- control plane writes it. `webhooks` is a JSON array, defaulting to '[]' so an
-- INSERT may omit it. Boolean-ish columns use INTEGER (0/1) so the sqlx `Any`
-- driver reads one uniform integer type across SQLite/MySQL/Postgres.
CREATE TABLE IF NOT EXISTS apps (
    id          TEXT    NOT NULL PRIMARY KEY,
    key         TEXT    NOT NULL UNIQUE,
    secret      TEXT    NOT NULL,
    name        TEXT    NOT NULL DEFAULT '',
    capacity    INTEGER NOT NULL DEFAULT 0,
    client_messages_enabled     INTEGER NOT NULL DEFAULT 0,
    subscription_count_enabled  INTEGER NOT NULL DEFAULT 0,
    enabled     INTEGER NOT NULL DEFAULT 1,
    webhooks    TEXT    NOT NULL DEFAULT '[]',
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
