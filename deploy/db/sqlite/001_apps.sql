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
