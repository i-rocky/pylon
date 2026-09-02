//! Server-level configuration (defaults + `PYLON_*` env overrides).

/// Which app-manager backend to use for app lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum AppManagerKind {
    StaticFile,
    Sqlite,
    Mysql,
    Postgres,
    Mongo,
}

/// Parse `PYLON_APP_MANAGER` into an [`AppManagerKind`].
/// Unknown values fall back to `StaticFile` (safe default).
pub fn parse_app_manager(kind: Option<&str>) -> AppManagerKind {
    match kind.unwrap_or("static") {
        "sqlite" => AppManagerKind::Sqlite,
        "mysql" => AppManagerKind::Mysql,
        "postgres" => AppManagerKind::Postgres,
        "mongo" => AppManagerKind::Mongo,
        _ => AppManagerKind::StaticFile,
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_presence_members: usize,
    pub max_event_payload_bytes: usize,
    pub max_watchlist_size: usize,
    pub max_channel_name_length: usize,
    pub max_event_name_length: usize,
    pub max_client_events_per_second: u32,
    pub max_presence_user_id_length: usize,
    pub max_presence_user_info_bytes: usize,
    pub max_subscriptions_per_connection: usize,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub activity_timeout: u32,
    pub pong_timeout: u32,
    /// Maximum connection age in seconds: a connection is closed with code
    /// **4202** ("Closed after inactivity", Pusher's reconnect-immediately
    /// band) once it has been established for this long, regardless of
    /// activity. Mirrors Pusher's 24-hour maximum connection lifetime.
    /// `0` disables the lifetime close. `PYLON_MAX_CONN_LIFETIME_SECS`.
    pub max_conn_lifetime_secs: u64,
    /// G3 (slowloris): maximum accepted HTTP request-head size (bytes) — the
    /// bytes accumulated while sniffing the WS upgrade / REST request head. A
    /// connection whose head exceeds this is closed (a client dribbling
    /// headerless bytes can no longer grow `inbuf` without bound). Generous
    /// against any legitimate head. `0` disables the cap.
    /// `PYLON_MAX_HEAD_BYTES` (default 16384).
    pub max_head_bytes: usize,
    /// G3 (slowloris): absolute deadline (ms) from ACCEPT within which a
    /// connection must complete its handshake (HTTP head + TLS + WS upgrade +
    /// session establish). On expiry the connection is closed and its slot
    /// reclaimed — inbound dribble does NOT postpone it. `0` disables the
    /// deadline. `PYLON_HANDSHAKE_TIMEOUT_MS` (default 10000).
    pub handshake_timeout_ms: u64,
    pub strict_protocol: bool,
    pub apps_path: String,
    pub max_presence_members: usize,
    pub max_event_payload_bytes: usize,
    pub max_channels_per_publish: usize,
    pub rest_auth_window_secs: u64,
    pub max_batch_events: usize,
    pub cache_ttl_secs: u64,
    pub max_watchlist_size: usize,
    pub webhook_batch_ms: u64,
    pub webhook_timeout_ms: u64,
    /// Exponential-backoff base for webhook retries: the delay after the first
    /// failed attempt is this long, doubling each attempt up to
    /// [`ServerConfig::webhook_backoff_cap_ms`]. `PYLON_WEBHOOK_BACKOFF_BASE_MS`
    /// (default 1000). Replaces the deprecated `webhook_retry_base_ms`.
    pub webhook_backoff_base_ms: u64,
    /// Upper bound (ms) for each webhook retry backoff delay (default 60000).
    /// `PYLON_WEBHOOK_BACKOFF_CAP_MS`.
    pub webhook_backoff_cap_ms: u64,
    /// Total webhook retry budget (ms): a delivery that has not received a 2xx
    /// by the time this much elapsed time (attempts + backoffs) has passed is
    /// dropped. Pusher parity — Channels "will retry sending the webhook, with
    /// exponential backoff, for 5 minutes" on any non-2xx. `0` disables retries
    /// (single attempt). `PYLON_WEBHOOK_RETRY_BUDGET_MS` (default 300000).
    pub webhook_retry_budget_ms: u64,
    pub webhook_max_concurrency: usize,
    /// S2: allow webhook delivery to private/loopback/link-local targets
    /// (SSRF guard escape hatch). `false` (default, secure): a delivery whose
    /// target resolves to (or literally is) a private address is refused
    /// before any HTTP is sent. `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS`
    /// (`1`/`true` to enable) — for operators whose webhook receivers live on
    /// internal networks.
    pub webhook_allow_private_targets: bool,
    pub max_channel_name_length: usize,
    pub max_event_name_length: usize,
    pub max_client_events_per_second: u32,
    pub max_presence_user_id_length: usize,
    pub max_presence_user_info_bytes: usize,
    pub max_subscriptions_per_connection: usize,
    // adapter + redis scaling tunables
    pub adapter: String,
    pub redis_url: String,
    pub redis_prefix: String,
    pub redis_pool_size: u32,
    pub redis_membership_ttl_secs: u64,
    pub redis_presence_heartbeat_secs: u64,
    pub redis_node_heartbeat_secs: u64,
    pub redis_sweep_interval_secs: u64,
    pub webhook_vacated_grace_ms: u64,
    pub redis_sharded_pubsub: bool,
    /// F-1: emit the Redis cluster relay envelope in its compat (double-carry)
    /// shape — every frame-carrying envelope travels with BOTH the legacy
    /// `event` escaped-JSON string field and the additive `frame_b64` field.
    /// Receivers always prefer `frame_b64` and fall back to `event`, so the
    /// default keeps a 0.2.x↔0.3.x mixed fleet relaying in both directions
    /// during rolling upgrades. Set `0`/`false`/`off`
    /// (`PYLON_CLUSTER_ENVELOPE_COMPAT`) — ONLY once EVERY node runs a build
    /// that SHIPS this knob — to omit the legacy `event` member for frame
    /// kinds and roughly halve the cluster-bus bandwidth. v0.3.0 alone does
    /// NOT qualify: a 0.3.0 receiver still requires the `event` field and
    /// SILENTLY DROPS compat-off envelopes (the missing-field decode
    /// accommodation shipped with this knob). Frame-less control envelopes
    /// keep their shape either way.
    pub cluster_envelope_compat: bool,
    /// Number of per-core worker threads for the percore transport. `0` means
    /// "auto" — one worker per available CPU. See [`ServerConfig::worker_count`].
    pub workers: usize,
    // ── SP10 adaptive overload (all auto-derived; overrides only) ──────────────
    /// Total memory budget in bytes for the percore transport. `0` (default) ⇒
    /// auto: `memory_budget(detect_effective_mem())`. `PYLON_MEMORY_BUDGET_BYTES`.
    pub memory_budget_bytes: u64,
    /// Memory budget as a fraction of the effective envelope (0.0..=1.0), applied
    /// when `memory_budget_bytes == 0`. `0.0` (default) ⇒ use the
    /// `max(1.5 GiB, 7%)` reserve formula instead. `PYLON_MEMORY_BUDGET_FRACTION`.
    pub memory_budget_fraction: f64,
    /// Expected concurrent connections per worker, used to derive the
    /// per-connection out-queue cap. `PYLON_EXPECTED_CONNS_PER_WORKER` (default 50_000).
    pub expected_conns_per_worker: u64,
    /// Lower clamp for the per-connection out-queue cap (bytes).
    /// `PYLON_PERCONN_QUEUE_MIN_BYTES` (default 256 KiB).
    pub perconn_queue_min_bytes: u64,
    /// Upper clamp for the per-connection out-queue cap (bytes).
    /// `PYLON_PERCONN_QUEUE_MAX_BYTES` (default 8 MiB).
    pub perconn_queue_max_bytes: u64,
    /// Capacity (frames) of each worker's bounded broadcast hand-off channel.
    /// `PYLON_BROADCAST_HANDOFF_CAP` (default 1024).
    pub broadcast_handoff_cap: usize,
    /// CoDel freshness target in milliseconds (§7): a frame whose time-in-queue
    /// (sojourn) exceeds `2 ×` this while the queue is overloaded is dropped on
    /// dequeue. `PYLON_CODEL_TARGET_MS` (default 5). `0` disables CoDel.
    pub codel_target_ms: u64,
    /// CoDel interval in milliseconds (§7): the window over which the minimum
    /// sojourn is tracked. `PYLON_CODEL_INTERVAL_MS` (default 100).
    pub codel_interval_ms: u64,
    /// PSI memory-pressure backstop (§8). `None` = auto (enabled if the kernel
    /// pressure file is readable at startup, else a no-op); `Some(true)`/
    /// `Some(false)` force on/off. `PYLON_PSI_BACKSTOP` (`1`/`true` / `0`/`false`).
    pub psi_backstop: Option<bool>,
    /// PSI `full avg10` threshold (percent) above which the budget factor is
    /// shrunk. `PYLON_PSI_THRESHOLD` (default 15.0).
    pub psi_threshold: f64,
    /// C2a: how long (ms) each percore worker waits for in-flight connections to
    /// drain before force-closing. After `shutdown` is set workers queue a
    /// `pusher:error` 4200 text frame plus a WS Close(4200) on every open
    /// connection, then keep flushing until
    /// `inflight_bytes == 0` OR this deadline passes, then clean up and exit.
    /// `PYLON_SHUTDOWN_GRACE_MS` (default `10000`).
    pub shutdown_grace_ms: u64,
    /// C2a: how long (ms) to wait after setting `draining=true` (→ `/ready` 503)
    /// before setting `shutdown=true` (→ workers begin their bounded drain). Gives
    /// load balancers time to observe `/ready`=503 and stop sending new traffic.
    /// `PYLON_SHUTDOWN_PREDRAIN_MS` (default `2000`).
    pub shutdown_predrain_ms: u64,
    // ── Node-level connection ceiling ────────────────────────────────────────
    /// Maximum simultaneous connections this node will accept, across all apps.
    /// `0` (default) ⇒ auto: `memory_budget_bytes / expected_per_conn_bytes`
    /// (if the budget is 0/unknown, the effective ceiling is 0 = unlimited).
    /// `PYLON_MAX_CONNECTIONS`.
    pub max_connections: usize,
    /// Expected per-connection memory footprint (bytes), used to auto-derive
    /// `max_connections` from the memory budget when `max_connections == 0`.
    /// `PYLON_EXPECTED_PER_CONN_BYTES` (default 8192).
    pub expected_per_conn_bytes: usize,
    // ── TLS (native rustls, optional) ───────────────────────────────────────
    /// Path to the PEM certificate chain. Must be set together with `tls_key_path`
    /// to enable TLS. Setting only one of cert/key is a fatal config error.
    /// `PYLON_TLS_CERT`. Empty string treated as None.
    pub tls_cert_path: Option<String>,
    /// Path to the PEM private key (PKCS#8, RSA, or EC). Must be set together
    /// with `tls_cert_path` to enable TLS. `PYLON_TLS_KEY`. Empty string treated
    /// as None.
    pub tls_key_path: Option<String>,
    /// Optional path to a PEM CA certificate for mTLS client verification.
    /// Enabling mTLS requires both `tls_cert_path` and `tls_key_path` to be set.
    /// `PYLON_TLS_CA`. Empty string treated as None.
    pub tls_ca_path: Option<String>,
    /// Capacity (frames) of each per-connection mailbox (the bounded channel used
    /// for cross-connection direct sends: presence rosters, `member_added/removed`,
    /// `send_to_user`, watchlist events, cluster bridge deliveries). When the mailbox
    /// is full a `try_send` silently drops the frame and bumps the per-worker
    /// `pylon_mailbox_dropped_total` counter. `PYLON_MAILBOX_CAPACITY` (default 256).
    pub mailbox_capacity: usize,
    /// Which app-manager backend to use. `PYLON_APP_MANAGER` (`static` | `sqlite` | `mysql` | `postgres` | `mongo`;
    /// default `static`). Set to `sqlite`, `mysql`, `postgres`, or `mongo` and provide `PYLON_APP_DSN`.
    pub app_manager: AppManagerKind,
    /// DSN for the DB-backed app manager. Required when `app_manager` is Sqlite, Mysql, Postgres, or Mongo.
    /// `PYLON_APP_DSN` (e.g. `sqlite:///var/lib/pylon/apps.db`).
    pub app_dsn: Option<String>,
    /// Enable the L1 (moka) + optional L2 (Redis) app-manager cache layer.
    /// `PYLON_APP_CACHE` (`0`/`off`/`false` to disable; default `true` / enabled).
    pub app_cache: bool,
    /// L1 moka cache max-capacity (apps). `PYLON_APP_CACHE_MAX` (default 100_000).
    pub app_cache_max: u64,
    /// L1 positive-entry TTL in seconds. `PYLON_APP_CACHE_TTL` (default 300).
    pub app_cache_ttl: u64,
    /// L1 negative-entry max-capacity. `PYLON_APP_CACHE_NEG_MAX` (default 10_000).
    pub app_cache_neg_max: u64,
    /// L1 negative-entry TTL in seconds. `PYLON_APP_CACHE_NEG_TTL` (default 30).
    pub app_cache_neg_ttl: u64,
    /// Redis URL for the L2 app cache. When set, resolved apps are written through
    /// to Redis and read back on L1 misses. `PYLON_APP_CACHE_REDIS_URL`.
    pub app_cache_redis_url: Option<String>,
    /// Bearer token required for the admin endpoints (`POST /admin/apps/{id}/invalidate`).
    /// When `None` (default, `PYLON_ADMIN_TOKEN` not set), the admin API is disabled (404).
    pub app_admin_token: Option<String>,
    /// S1: optional bearer token for `GET /metrics`. When `None` (default,
    /// `PYLON_METRICS_TOKEN` not set) metrics stay open (back-compat). When set,
    /// a scrape must carry `Authorization: Bearer <token>`; anything else gets
    /// **404** (not 401 — an unauthenticated prober must not learn that the
    /// endpoint exists). `/health` and `/ready` are never gated (LB probes).
    /// An empty value is treated as unset. `PYLON_METRICS_TOKEN`.
    pub metrics_token: Option<String>,
    /// Interval (seconds) for the app-purge sweep backstop. `0` disables it
    /// (default). `PYLON_APP_SWEEP_INTERVAL`. The sweep enumerates distinct
    /// connected apps and purges any the authoritative (uncached) driver reports
    /// as absent/disabled — closing the gap if a `remove` signal is ever missed.
    pub app_sweep_interval_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".into(),
            port: 7000,
            activity_timeout: 120,
            pong_timeout: 30,
            max_conn_lifetime_secs: 86_400,
            max_head_bytes: 16_384,
            handshake_timeout_ms: 10_000,
            strict_protocol: false,
            apps_path: "apps.json".into(),
            max_presence_members: 100,
            max_event_payload_bytes: 10_240,
            max_channels_per_publish: 100,
            rest_auth_window_secs: 600,
            max_batch_events: 10,
            cache_ttl_secs: 1800,
            max_watchlist_size: 100,
            webhook_batch_ms: 50,
            webhook_timeout_ms: 5000,
            webhook_backoff_base_ms: 1000,
            webhook_backoff_cap_ms: 60000,
            webhook_retry_budget_ms: 300000,
            webhook_max_concurrency: 100,
            webhook_allow_private_targets: false,
            max_channel_name_length: 164,
            max_event_name_length: 200,
            max_client_events_per_second: 10,
            max_presence_user_id_length: 128,
            max_presence_user_info_bytes: 1024,
            max_subscriptions_per_connection: 200,
            adapter: "local".into(),
            redis_url: "redis://127.0.0.1:6379".into(),
            redis_prefix: "pylon".into(),
            redis_pool_size: 6,
            redis_membership_ttl_secs: 60,
            redis_presence_heartbeat_secs: 25,
            redis_node_heartbeat_secs: 5,
            redis_sweep_interval_secs: 10,
            webhook_vacated_grace_ms: 3000,
            redis_sharded_pubsub: false,
            cluster_envelope_compat: true,
            workers: 0,
            memory_budget_bytes: 0,
            memory_budget_fraction: 0.0,
            expected_conns_per_worker: 50_000,
            perconn_queue_min_bytes: 256 << 10,
            perconn_queue_max_bytes: 8 << 20,
            broadcast_handoff_cap: crate::transport::fanout::DEFAULT_BROADCAST_HANDOFF_CAP,
            codel_target_ms: 5,
            codel_interval_ms: 100,
            psi_backstop: None,
            psi_threshold: 15.0,
            shutdown_grace_ms: 10_000,
            shutdown_predrain_ms: 2_000,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            max_connections: 0,
            expected_per_conn_bytes: 8192,
            mailbox_capacity: 256,
            app_manager: AppManagerKind::StaticFile,
            app_dsn: None,
            app_cache: true,
            app_cache_max: 100_000,
            app_cache_ttl: 300,
            app_cache_neg_max: 10_000,
            app_cache_neg_ttl: 30,
            app_cache_redis_url: None,
            app_admin_token: None,
            metrics_token: None,
            app_sweep_interval_secs: 0,
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("PYLON_BIND") {
            c.bind = v;
        }
        if let Ok(v) = std::env::var("PYLON_PORT") {
            if let Ok(p) = v.parse() {
                c.port = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_ACTIVITY_TIMEOUT") {
            if let Ok(p) = v.parse() {
                c.activity_timeout = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_PONG_TIMEOUT") {
            if let Ok(p) = v.parse() {
                c.pong_timeout = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_CONN_LIFETIME_SECS") {
            if let Ok(p) = v.parse() {
                c.max_conn_lifetime_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_HEAD_BYTES") {
            if let Ok(p) = v.parse() {
                c.max_head_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_HANDSHAKE_TIMEOUT_MS") {
            if let Ok(p) = v.parse() {
                c.handshake_timeout_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_STRICT_PROTOCOL") {
            c.strict_protocol = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("PYLON_APPS_PATH") {
            c.apps_path = v;
        }
        c.app_manager = parse_app_manager(std::env::var("PYLON_APP_MANAGER").ok().as_deref());
        c.app_dsn = std::env::var("PYLON_APP_DSN").ok();
        if let Ok(v) = std::env::var("PYLON_APP_CACHE") {
            c.app_cache = v != "0" && v.to_lowercase() != "off" && v.to_lowercase() != "false";
        }
        if let Ok(v) = std::env::var("PYLON_APP_CACHE_MAX") {
            if let Ok(n) = v.parse() {
                c.app_cache_max = n;
            }
        }
        if let Ok(v) = std::env::var("PYLON_APP_CACHE_TTL") {
            if let Ok(n) = v.parse() {
                c.app_cache_ttl = n;
            }
        }
        if let Ok(v) = std::env::var("PYLON_APP_CACHE_NEG_MAX") {
            if let Ok(n) = v.parse() {
                c.app_cache_neg_max = n;
            }
        }
        if let Ok(v) = std::env::var("PYLON_APP_CACHE_NEG_TTL") {
            if let Ok(n) = v.parse() {
                c.app_cache_neg_ttl = n;
            }
        }
        c.app_cache_redis_url = std::env::var("PYLON_APP_CACHE_REDIS_URL").ok();
        c.app_admin_token = std::env::var("PYLON_ADMIN_TOKEN").ok();
        // S1: empty string is treated as "not set" (same convention as the TLS
        // paths) so `PYLON_METRICS_TOKEN=` can never arm a gate whose token is
        // the empty string.
        if let Ok(v) = std::env::var("PYLON_METRICS_TOKEN") {
            if !v.is_empty() {
                c.metrics_token = Some(v);
            }
        }
        if let Ok(v) = std::env::var("PYLON_APP_SWEEP_INTERVAL") {
            if let Ok(n) = v.parse() {
                c.app_sweep_interval_secs = n;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_PRESENCE_MEMBERS") {
            if let Ok(p) = v.parse() {
                c.max_presence_members = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_EVENT_PAYLOAD_BYTES") {
            if let Ok(p) = v.parse() {
                c.max_event_payload_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_CHANNELS_PER_PUBLISH") {
            if let Ok(p) = v.parse() {
                c.max_channels_per_publish = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_REST_AUTH_WINDOW_SECS") {
            if let Ok(p) = v.parse() {
                c.rest_auth_window_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_BATCH_EVENTS") {
            if let Ok(p) = v.parse() {
                c.max_batch_events = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_CACHE_TTL_SECS") {
            if let Ok(p) = v.parse() {
                c.cache_ttl_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_WATCHLIST_SIZE") {
            if let Ok(p) = v.parse() {
                c.max_watchlist_size = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_BATCH_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_batch_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_TIMEOUT_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_timeout_ms = p;
            }
        }
        // Deprecated webhook retry knobs (kept for one release). Parsed FIRST so
        // the canonical `PYLON_WEBHOOK_BACKOFF_BASE_MS` wins when both are set;
        // `PYLON_WEBHOOK_MAX_RETRIES` has no successor (retries are bounded by
        // the total-time budget now, not by an attempt count) and is ignored.
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_RETRY_BASE_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_backoff_base_ms = p;
            }
            tracing::warn!(
                value = %v,
                "PYLON_WEBHOOK_RETRY_BASE_MS is deprecated; use PYLON_WEBHOOK_BACKOFF_BASE_MS \
                 (alias honored this release)"
            );
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_MAX_RETRIES") {
            tracing::warn!(
                value = %v,
                "PYLON_WEBHOOK_MAX_RETRIES is deprecated and ignored; webhook retries are now \
                 bounded by PYLON_WEBHOOK_RETRY_BUDGET_MS (total time, not attempt count)"
            );
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_BACKOFF_BASE_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_backoff_base_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_BACKOFF_CAP_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_backoff_cap_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_RETRY_BUDGET_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_retry_budget_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_MAX_CONCURRENCY") {
            if let Ok(p) = v.parse() {
                c.webhook_max_concurrency = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS") {
            c.webhook_allow_private_targets = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("PYLON_MAX_CHANNEL_NAME_LENGTH") {
            if let Ok(p) = v.parse() {
                c.max_channel_name_length = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_EVENT_NAME_LENGTH") {
            if let Ok(p) = v.parse() {
                c.max_event_name_length = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_CLIENT_EVENTS_PER_SECOND") {
            if let Ok(p) = v.parse() {
                c.max_client_events_per_second = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_PRESENCE_USER_ID_LENGTH") {
            if let Ok(p) = v.parse() {
                c.max_presence_user_id_length = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_PRESENCE_USER_INFO_BYTES") {
            if let Ok(p) = v.parse() {
                c.max_presence_user_info_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_SUBSCRIPTIONS_PER_CONNECTION") {
            if let Ok(p) = v.parse() {
                c.max_subscriptions_per_connection = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_ADAPTER") {
            c.adapter = v;
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_URL") {
            c.redis_url = v;
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_PREFIX") {
            c.redis_prefix = v;
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_POOL_SIZE") {
            if let Ok(p) = v.parse() {
                c.redis_pool_size = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_MEMBERSHIP_TTL") {
            if let Ok(p) = v.parse() {
                c.redis_membership_ttl_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_PRESENCE_HEARTBEAT") {
            if let Ok(p) = v.parse() {
                c.redis_presence_heartbeat_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_NODE_HEARTBEAT") {
            if let Ok(p) = v.parse() {
                c.redis_node_heartbeat_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_SWEEP_INTERVAL") {
            if let Ok(p) = v.parse() {
                c.redis_sweep_interval_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_VACATED_GRACE_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_vacated_grace_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_REDIS_SHARDED_PUBSUB") {
            c.redis_sharded_pubsub = v == "1" || v.eq_ignore_ascii_case("true");
        }
        // F-1: default-true knob (the app_cache parsing shape, `off` included)
        // — anything but an explicit `0`/`false`/`off` keeps the compat
        // double-carry wire shape.
        if let Ok(v) = std::env::var("PYLON_CLUSTER_ENVELOPE_COMPAT") {
            c.cluster_envelope_compat =
                v != "0" && v.to_lowercase() != "off" && v.to_lowercase() != "false";
        }
        if let Ok(v) = std::env::var("PYLON_WORKERS") {
            if let Ok(p) = v.parse() {
                c.workers = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MEMORY_BUDGET_BYTES") {
            if let Ok(p) = v.parse() {
                c.memory_budget_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MEMORY_BUDGET_FRACTION") {
            if let Ok(p) = v.parse() {
                c.memory_budget_fraction = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_EXPECTED_CONNS_PER_WORKER") {
            if let Ok(p) = v.parse() {
                c.expected_conns_per_worker = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_PERCONN_QUEUE_MIN_BYTES") {
            if let Ok(p) = v.parse() {
                c.perconn_queue_min_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_PERCONN_QUEUE_MAX_BYTES") {
            if let Ok(p) = v.parse() {
                c.perconn_queue_max_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_BROADCAST_HANDOFF_CAP") {
            if let Ok(p) = v.parse() {
                c.broadcast_handoff_cap = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_CODEL_TARGET_MS") {
            if let Ok(p) = v.parse() {
                c.codel_target_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_CODEL_INTERVAL_MS") {
            if let Ok(p) = v.parse() {
                c.codel_interval_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_PSI_BACKSTOP") {
            c.psi_backstop = Some(v == "1" || v.eq_ignore_ascii_case("true"));
        }
        if let Ok(v) = std::env::var("PYLON_PSI_THRESHOLD") {
            if let Ok(p) = v.parse() {
                c.psi_threshold = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_SHUTDOWN_GRACE_MS") {
            if let Ok(p) = v.parse() {
                c.shutdown_grace_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_SHUTDOWN_PREDRAIN_MS") {
            if let Ok(p) = v.parse() {
                c.shutdown_predrain_ms = p;
            }
        }
        // TLS — empty string is treated as "not set" (same as absent).
        if let Ok(v) = std::env::var("PYLON_TLS_CERT") {
            if !v.is_empty() {
                c.tls_cert_path = Some(v);
            }
        }
        if let Ok(v) = std::env::var("PYLON_TLS_KEY") {
            if !v.is_empty() {
                c.tls_key_path = Some(v);
            }
        }
        if let Ok(v) = std::env::var("PYLON_TLS_CA") {
            if !v.is_empty() {
                c.tls_ca_path = Some(v);
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_CONNECTIONS") {
            if let Ok(p) = v.parse() {
                c.max_connections = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_EXPECTED_PER_CONN_BYTES") {
            if let Ok(p) = v.parse() {
                c.expected_per_conn_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAILBOX_CAPACITY") {
            if let Ok(p) = v.parse::<usize>() {
                if p > 0 {
                    c.mailbox_capacity = p;
                }
            }
        }
        c
    }

    /// Resolve the CoDel parameters for the percore transport. A `codel_target_ms`
    /// of `0` yields the disabled overlay (pure drop-head); the interval is
    /// clamped to a sane minimum so a misconfigured `0` interval can't divide the
    /// window to nothing.
    pub fn codel_params(&self) -> crate::transport::conn::CodelParams {
        crate::transport::conn::CodelParams {
            target_ns: self.codel_target_ms.saturating_mul(1_000_000),
            interval_ns: self.codel_interval_ms.max(1).saturating_mul(1_000_000),
        }
    }

    /// Resolve the percore total memory budget (bytes): the explicit
    /// `memory_budget_bytes` override if non-zero; else the configured fraction
    /// of `effective_mem` if set; else the `max(1.5 GiB, 7%)` reserve formula.
    pub fn resolved_memory_budget(&self, effective_mem: u64) -> u64 {
        if self.memory_budget_bytes != 0 {
            self.memory_budget_bytes
        } else if self.memory_budget_fraction > 0.0 {
            ((effective_mem as f64) * self.memory_budget_fraction) as u64
        } else {
            crate::server::resources::memory_budget(effective_mem)
        }
    }

    /// Resolve the per-core worker count: the configured value, or — when `0`
    /// ("auto") — the number of available CPUs (falling back to `1` if the OS
    /// won't report it).
    pub fn worker_count(&self) -> usize {
        if self.workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            self.workers
        }
    }

    /// Resolve the effective node connection ceiling.
    ///
    /// * If `max_connections != 0`, return it as-is (explicit override).
    /// * Else derive from budget: `memory_budget_bytes / expected_per_conn_bytes`.
    ///   The caller supplies `budget` (already resolved via
    ///   [`Self::resolved_memory_budget`]) so this method is pure.
    ///   If `budget == 0` (budget unknown), returns `0` = unlimited.
    pub fn resolved_max_connections(&self, budget: u64) -> usize {
        if self.max_connections != 0 {
            self.max_connections
        } else if budget != 0 && self.expected_per_conn_bytes != 0 {
            (budget / self.expected_per_conn_bytes as u64) as usize
        } else {
            0
        }
    }

    pub fn limits(&self) -> Limits {
        Limits {
            max_presence_members: self.max_presence_members,
            max_event_payload_bytes: self.max_event_payload_bytes,
            max_watchlist_size: self.max_watchlist_size,
            max_channel_name_length: self.max_channel_name_length,
            max_event_name_length: self.max_event_name_length,
            max_client_events_per_second: self.max_client_events_per_second,
            max_presence_user_id_length: self.max_presence_user_id_length,
            max_presence_user_info_bytes: self.max_presence_user_info_bytes,
            max_subscriptions_per_connection: self.max_subscriptions_per_connection,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_spec() {
        let c = ServerConfig::default();
        assert_eq!(c.port, 7000);
        assert_eq!(c.activity_timeout, 120);
        assert_eq!(c.pong_timeout, 30);
        // Max connection lifetime: Pusher parity — 24h default, 0 = disabled.
        assert_eq!(c.max_conn_lifetime_secs, 86_400);
        // G3 slowloris hardening: 16 KiB head cap, 10s handshake deadline.
        assert_eq!(c.max_head_bytes, 16_384);
        assert_eq!(c.handshake_timeout_ms, 10_000);
        assert!(!c.strict_protocol);
        assert_eq!(c.max_presence_members, 100);
        assert_eq!(c.max_event_payload_bytes, 10_240);
        assert_eq!(c.max_channels_per_publish, 100);
        assert_eq!(c.rest_auth_window_secs, 600);
        assert_eq!(c.max_batch_events, 10);
        assert_eq!(c.cache_ttl_secs, 1800);
        assert_eq!(c.max_watchlist_size, 100);
        assert_eq!(c.max_channel_name_length, 164);
        assert_eq!(c.max_event_name_length, 200);
        assert_eq!(c.max_client_events_per_second, 10);
        assert_eq!(c.max_presence_user_id_length, 128);
        assert_eq!(c.max_presence_user_info_bytes, 1024);
        // webhook tunables (spec §6 + Pusher retry parity: exponential backoff
        // 1s doubling to a 60s cap, for a ~5-minute total budget).
        assert_eq!(c.webhook_batch_ms, 50);
        assert_eq!(c.webhook_timeout_ms, 5000);
        assert_eq!(c.webhook_backoff_base_ms, 1000);
        assert_eq!(c.webhook_backoff_cap_ms, 60000);
        assert_eq!(c.webhook_retry_budget_ms, 300000);
        assert_eq!(c.webhook_max_concurrency, 100);
        // S2 SSRF guard: private webhook targets refused by default.
        assert!(!c.webhook_allow_private_targets);
        // adapter + redis tunables
        assert_eq!(c.adapter, "local");
        assert_eq!(c.redis_url, "redis://127.0.0.1:6379");
        assert_eq!(c.redis_prefix, "pylon");
        assert_eq!(c.redis_pool_size, 6);
        assert_eq!(c.redis_membership_ttl_secs, 60);
        assert_eq!(c.redis_presence_heartbeat_secs, 25);
        assert_eq!(c.redis_node_heartbeat_secs, 5);
        assert_eq!(c.redis_sweep_interval_secs, 10);
        assert_eq!(c.webhook_vacated_grace_ms, 3000);
        assert!(!c.redis_sharded_pubsub);
        // F-1: compat double-carry is on by default (mixed-fleet safety).
        assert!(c.cluster_envelope_compat);
        // SP10 adaptive-overload defaults (all auto).
        assert_eq!(c.memory_budget_bytes, 0);
        assert_eq!(c.memory_budget_fraction, 0.0);
        assert_eq!(c.expected_conns_per_worker, 50_000);
        assert_eq!(c.perconn_queue_min_bytes, 256 << 10);
        assert_eq!(c.perconn_queue_max_bytes, 8 << 20);
        assert_eq!(c.broadcast_handoff_cap, 1024);
        // SP10 §7/§8 CoDel + PSI defaults.
        assert_eq!(c.codel_target_ms, 5);
        assert_eq!(c.codel_interval_ms, 100);
        assert_eq!(c.psi_backstop, None); // auto
        assert_eq!(c.psi_threshold, 15.0);
        // C2a graceful-shutdown defaults.
        assert_eq!(c.shutdown_grace_ms, 10_000);
        assert_eq!(c.shutdown_predrain_ms, 2_000);
        // TLS defaults — all off.
        assert!(c.tls_cert_path.is_none());
        assert!(c.tls_key_path.is_none());
        assert!(c.tls_ca_path.is_none());
        // codel_params() folds ms → ns with the folly defaults.
        let p = c.codel_params();
        assert_eq!(p.target_ns, 5_000_000);
        assert_eq!(p.interval_ns, 100_000_000);
        // target_ms = 0 ⇒ disabled overlay (target_ns == 0).
        let mut off = c.clone();
        off.codel_target_ms = 0;
        assert_eq!(off.codel_params().target_ns, 0);
    }

    #[test]
    fn sp10_overload_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_MEMORY_BUDGET_BYTES", "67108864");
        std::env::set_var("PYLON_EXPECTED_CONNS_PER_WORKER", "1000");
        std::env::set_var("PYLON_PERCONN_QUEUE_MIN_BYTES", "4096");
        std::env::set_var("PYLON_PERCONN_QUEUE_MAX_BYTES", "1048576");
        std::env::set_var("PYLON_BROADCAST_HANDOFF_CAP", "16");
        let c = ServerConfig::from_env();
        assert_eq!(c.memory_budget_bytes, 67_108_864);
        assert_eq!(c.expected_conns_per_worker, 1000);
        assert_eq!(c.perconn_queue_min_bytes, 4096);
        assert_eq!(c.perconn_queue_max_bytes, 1_048_576);
        assert_eq!(c.broadcast_handoff_cap, 16);
        // An explicit byte budget wins over the formula.
        assert_eq!(c.resolved_memory_budget(8u64 << 30), 67_108_864);
        std::env::remove_var("PYLON_MEMORY_BUDGET_BYTES");
        std::env::remove_var("PYLON_EXPECTED_CONNS_PER_WORKER");
        std::env::remove_var("PYLON_PERCONN_QUEUE_MIN_BYTES");
        std::env::remove_var("PYLON_PERCONN_QUEUE_MAX_BYTES");
        std::env::remove_var("PYLON_BROADCAST_HANDOFF_CAP");
    }

    #[test]
    fn sp10_codel_psi_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_CODEL_TARGET_MS", "0"); // disables CoDel
        std::env::set_var("PYLON_CODEL_INTERVAL_MS", "250");
        std::env::set_var("PYLON_PSI_BACKSTOP", "false");
        std::env::set_var("PYLON_PSI_THRESHOLD", "30");
        let c = ServerConfig::from_env();
        assert_eq!(c.codel_target_ms, 0);
        assert_eq!(c.codel_interval_ms, 250);
        assert_eq!(c.psi_backstop, Some(false));
        assert_eq!(c.psi_threshold, 30.0);
        // target_ms 0 ⇒ disabled overlay; interval still folds to ns.
        let p = c.codel_params();
        assert_eq!(p.target_ns, 0);
        assert_eq!(p.interval_ns, 250_000_000);
        std::env::remove_var("PYLON_CODEL_TARGET_MS");
        std::env::remove_var("PYLON_CODEL_INTERVAL_MS");
        std::env::remove_var("PYLON_PSI_BACKSTOP");
        std::env::remove_var("PYLON_PSI_THRESHOLD");
    }

    #[test]
    fn max_conn_lifetime_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Override …
        std::env::set_var("PYLON_MAX_CONN_LIFETIME_SECS", "3600");
        let c = ServerConfig::from_env();
        assert_eq!(c.max_conn_lifetime_secs, 3600);
        // … and the explicit disable value (0 = no lifetime close).
        std::env::set_var("PYLON_MAX_CONN_LIFETIME_SECS", "0");
        let c = ServerConfig::from_env();
        assert_eq!(c.max_conn_lifetime_secs, 0);
        std::env::remove_var("PYLON_MAX_CONN_LIFETIME_SECS");
    }

    #[test]
    fn slowloris_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_MAX_HEAD_BYTES", "4096");
        std::env::set_var("PYLON_HANDSHAKE_TIMEOUT_MS", "2500");
        let c = ServerConfig::from_env();
        assert_eq!(c.max_head_bytes, 4096);
        assert_eq!(c.handshake_timeout_ms, 2500);
        // The explicit disable values (0 = no cap / no deadline).
        std::env::set_var("PYLON_MAX_HEAD_BYTES", "0");
        std::env::set_var("PYLON_HANDSHAKE_TIMEOUT_MS", "0");
        let c = ServerConfig::from_env();
        assert_eq!(c.max_head_bytes, 0);
        assert_eq!(c.handshake_timeout_ms, 0);
        std::env::remove_var("PYLON_MAX_HEAD_BYTES");
        std::env::remove_var("PYLON_HANDSHAKE_TIMEOUT_MS");
    }

    #[test]
    fn resolved_budget_falls_back_to_formula() {
        let c = ServerConfig::default();
        // No override → the max(1.5 GiB, 7%) reserve formula.
        assert_eq!(
            c.resolved_memory_budget(4u64 << 30),
            crate::server::resources::memory_budget(4u64 << 30)
        );
    }

    #[test]
    fn redis_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_ADAPTER", "redis");
        std::env::set_var("PYLON_REDIS_URL", "redis://10.0.0.1:6379");
        std::env::set_var("PYLON_REDIS_PREFIX", "mypylon");
        std::env::set_var("PYLON_REDIS_POOL_SIZE", "12");
        std::env::set_var("PYLON_REDIS_MEMBERSHIP_TTL", "120");
        std::env::set_var("PYLON_REDIS_PRESENCE_HEARTBEAT", "50");
        std::env::set_var("PYLON_REDIS_NODE_HEARTBEAT", "10");
        std::env::set_var("PYLON_REDIS_SWEEP_INTERVAL", "20");
        std::env::set_var("PYLON_WEBHOOK_VACATED_GRACE_MS", "5000");
        std::env::set_var("PYLON_REDIS_SHARDED_PUBSUB", "true");
        let c = ServerConfig::from_env();
        assert_eq!(c.adapter, "redis");
        assert_eq!(c.redis_url, "redis://10.0.0.1:6379");
        assert_eq!(c.redis_prefix, "mypylon");
        assert_eq!(c.redis_pool_size, 12);
        assert_eq!(c.redis_membership_ttl_secs, 120);
        assert_eq!(c.redis_presence_heartbeat_secs, 50);
        assert_eq!(c.redis_node_heartbeat_secs, 10);
        assert_eq!(c.redis_sweep_interval_secs, 20);
        assert_eq!(c.webhook_vacated_grace_ms, 5000);
        assert!(c.redis_sharded_pubsub);
        std::env::remove_var("PYLON_ADAPTER");
        std::env::remove_var("PYLON_REDIS_URL");
        std::env::remove_var("PYLON_REDIS_PREFIX");
        std::env::remove_var("PYLON_REDIS_POOL_SIZE");
        std::env::remove_var("PYLON_REDIS_MEMBERSHIP_TTL");
        std::env::remove_var("PYLON_REDIS_PRESENCE_HEARTBEAT");
        std::env::remove_var("PYLON_REDIS_NODE_HEARTBEAT");
        std::env::remove_var("PYLON_REDIS_SWEEP_INTERVAL");
        std::env::remove_var("PYLON_WEBHOOK_VACATED_GRACE_MS");
        std::env::remove_var("PYLON_REDIS_SHARDED_PUBSUB");
    }

    #[test]
    fn node_ceiling_defaults_unlimited() {
        let c = ServerConfig::default();
        assert_eq!(c.max_connections, 0);
        assert_eq!(c.expected_per_conn_bytes, 8192);
        // max_connections == 0 with budget == 0 → unlimited (0).
        assert_eq!(c.resolved_max_connections(0), 0);
        // max_connections == 0 with non-zero budget → auto-derive.
        assert_eq!(c.resolved_max_connections(8192), 1);
    }

    #[test]
    fn node_ceiling_explicit_override_wins() {
        let c = ServerConfig {
            max_connections: 500,
            ..ServerConfig::default()
        };
        assert_eq!(c.resolved_max_connections(0), 500);
        assert_eq!(c.resolved_max_connections(1 << 30), 500);
    }

    #[test]
    fn node_ceiling_auto_from_budget() {
        let c = ServerConfig {
            expected_per_conn_bytes: 8192,
            ..ServerConfig::default()
        };
        // budget = 8 MiB → 8 MiB / 8 KiB = 1024 connections
        assert_eq!(c.resolved_max_connections(8 << 20), 1024);
        // budget = 0 → unlimited (0)
        assert_eq!(c.resolved_max_connections(0), 0);
    }

    #[test]
    fn node_ceiling_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_MAX_CONNECTIONS", "1000");
        std::env::set_var("PYLON_EXPECTED_PER_CONN_BYTES", "4096");
        let c = ServerConfig::from_env();
        assert_eq!(c.max_connections, 1000);
        assert_eq!(c.expected_per_conn_bytes, 4096);
        std::env::remove_var("PYLON_MAX_CONNECTIONS");
        std::env::remove_var("PYLON_EXPECTED_PER_CONN_BYTES");
    }

    #[test]
    fn workers_default_is_auto() {
        let c = ServerConfig::default();
        assert_eq!(c.workers, 0);
        // Auto resolves to >= 1 (available_parallelism, or the fallback of 1).
        assert!(c.worker_count() >= 1);
    }

    #[test]
    fn worker_count_uses_explicit_value() {
        let c = ServerConfig {
            workers: 4,
            ..ServerConfig::default()
        };
        assert_eq!(c.worker_count(), 4);
    }

    #[test]
    fn workers_env_override_applies() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_WORKERS", "3");
        let c = ServerConfig::from_env();
        assert_eq!(c.workers, 3);
        assert_eq!(c.worker_count(), 3);
        std::env::remove_var("PYLON_WORKERS");
    }

    #[test]
    fn parse_app_manager_maps_kinds() {
        assert_eq!(parse_app_manager(None), AppManagerKind::StaticFile);
        assert_eq!(
            parse_app_manager(Some("static")),
            AppManagerKind::StaticFile
        );
        assert_eq!(parse_app_manager(Some("sqlite")), AppManagerKind::Sqlite);
        assert_eq!(parse_app_manager(Some("mysql")), AppManagerKind::Mysql);
        assert_eq!(
            parse_app_manager(Some("postgres")),
            AppManagerKind::Postgres
        );
        assert_eq!(parse_app_manager(Some("mongo")), AppManagerKind::Mongo);
        assert_eq!(parse_app_manager(Some("weird")), AppManagerKind::StaticFile);
    }

    #[test]
    fn cache_defaults_are_sane() {
        let c = ServerConfig::default();
        assert!(c.app_cache); // on by default
        assert_eq!(c.app_cache_max, 100_000);
        assert_eq!(c.app_cache_ttl, 300);
        assert_eq!(c.app_cache_neg_ttl, 30);
    }

    #[test]
    fn admin_token_defaults_none() {
        assert!(ServerConfig::default().app_admin_token.is_none());
    }

    #[test]
    fn metrics_token_defaults_none() {
        // Secure-by-default in the back-compat sense: unset token = open metrics.
        assert!(ServerConfig::default().metrics_token.is_none());
    }

    #[test]
    fn metrics_token_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_METRICS_TOKEN", "opensesame");
        let c = ServerConfig::from_env();
        assert_eq!(c.metrics_token.as_deref(), Some("opensesame"));
        // An empty value is treated as unset (a locked-down-by-"" footgun).
        std::env::set_var("PYLON_METRICS_TOKEN", "");
        let c = ServerConfig::from_env();
        assert!(c.metrics_token.is_none(), "empty token must be None");
        std::env::remove_var("PYLON_METRICS_TOKEN");
    }

    #[test]
    fn tls_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_TLS_CERT", "/path/to/cert.pem");
        std::env::set_var("PYLON_TLS_KEY", "/path/to/key.pem");
        std::env::set_var("PYLON_TLS_CA", "/path/to/ca.pem");
        let c = ServerConfig::from_env();
        assert_eq!(c.tls_cert_path.as_deref(), Some("/path/to/cert.pem"));
        assert_eq!(c.tls_key_path.as_deref(), Some("/path/to/key.pem"));
        assert_eq!(c.tls_ca_path.as_deref(), Some("/path/to/ca.pem"));
        // Empty string is treated as absent (same test, same lock on the env vars
        // — avoids a parallel-test race on the shared process environment).
        std::env::set_var("PYLON_TLS_CERT", "");
        std::env::set_var("PYLON_TLS_KEY", "");
        std::env::set_var("PYLON_TLS_CA", "");
        let c2 = ServerConfig::from_env();
        assert!(
            c2.tls_cert_path.is_none(),
            "empty PYLON_TLS_CERT should be None"
        );
        assert!(
            c2.tls_key_path.is_none(),
            "empty PYLON_TLS_KEY should be None"
        );
        assert!(
            c2.tls_ca_path.is_none(),
            "empty PYLON_TLS_CA should be None"
        );
        std::env::remove_var("PYLON_TLS_CERT");
        std::env::remove_var("PYLON_TLS_KEY");
        std::env::remove_var("PYLON_TLS_CA");
    }

    /// Serializes tests that mutate `PYLON_*` env vars and read them back
    /// through `from_env`. The process environment and tracing's callsite
    /// interest cache are process-global, so parallel runs would race (a
    /// foreign thread evaluating a warn! callsite under the no-op global
    /// subscriber can cache it as disabled, dropping the event).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn webhook_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Use a guarded set/remove to avoid cross-test env bleed.
        std::env::set_var("PYLON_WEBHOOK_BATCH_MS", "25");
        std::env::set_var("PYLON_WEBHOOK_TIMEOUT_MS", "1234");
        std::env::set_var("PYLON_WEBHOOK_BACKOFF_BASE_MS", "10");
        std::env::set_var("PYLON_WEBHOOK_BACKOFF_CAP_MS", "20");
        std::env::set_var("PYLON_WEBHOOK_RETRY_BUDGET_MS", "30");
        std::env::set_var("PYLON_WEBHOOK_MAX_CONCURRENCY", "5");
        std::env::set_var("PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "1");
        let c = ServerConfig::from_env();
        assert_eq!(c.webhook_batch_ms, 25);
        assert_eq!(c.webhook_timeout_ms, 1234);
        assert_eq!(c.webhook_backoff_base_ms, 10);
        assert_eq!(c.webhook_backoff_cap_ms, 20);
        assert_eq!(c.webhook_retry_budget_ms, 30);
        assert_eq!(c.webhook_max_concurrency, 5);
        assert!(c.webhook_allow_private_targets, "the SSRF escape hatch");
        // "true" also enables; anything else leaves it off.
        std::env::set_var("PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "true");
        assert!(ServerConfig::from_env().webhook_allow_private_targets);
        std::env::set_var("PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "0");
        assert!(!ServerConfig::from_env().webhook_allow_private_targets);
        std::env::remove_var("PYLON_WEBHOOK_BATCH_MS");
        std::env::remove_var("PYLON_WEBHOOK_TIMEOUT_MS");
        std::env::remove_var("PYLON_WEBHOOK_BACKOFF_BASE_MS");
        std::env::remove_var("PYLON_WEBHOOK_BACKOFF_CAP_MS");
        std::env::remove_var("PYLON_WEBHOOK_RETRY_BUDGET_MS");
        std::env::remove_var("PYLON_WEBHOOK_MAX_CONCURRENCY");
        std::env::remove_var("PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS");
    }

    /// Install a thread-local `tracing` subscriber that records every event's
    /// `message` field into a shared buffer, so tests can assert that
    /// deprecated env vars emit a warning. A hand-rolled subscriber (rather
    /// than `fmt` + `MakeWriter`) keeps the interest protocol simple: events
    /// are unconditionally enabled, so nothing is dropped by level filtering.
    fn capture_warnings() -> (
        std::sync::Arc<std::sync::Mutex<String>>,
        tracing::subscriber::DefaultGuard,
    ) {
        use std::sync::{Arc, Mutex};

        struct CaptureSubscriber {
            events: Arc<Mutex<String>>,
        }

        struct MessageVisitor<'a> {
            out: &'a mut String,
        }

        impl tracing::field::Visit for MessageVisitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    use std::fmt::Write;
                    let _ = write!(self.out, "{value:?} ");
                }
            }
        }

        impl tracing::Subscriber for CaptureSubscriber {
            fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
                true
            }

            fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }

            fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

            fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {
            }

            fn event(&self, event: &tracing::Event<'_>) {
                let mut line = String::new();
                event.record(&mut MessageVisitor { out: &mut line });
                let mut events = self.events.lock().unwrap();
                events.push_str(&line);
                events.push('\n');
            }

            fn enter(&self, _span: &tracing::span::Id) {}

            fn exit(&self, _span: &tracing::span::Id) {}
        }

        let buf = Arc::new(Mutex::new(String::new()));
        let guard = tracing::subscriber::set_default(CaptureSubscriber {
            events: buf.clone(),
        });
        (buf, guard)
    }

    #[test]
    fn deprecated_webhook_retry_env_vars_alias_and_warn() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // The deprecated alias still configures the new base delay…
        std::env::set_var("PYLON_WEBHOOK_RETRY_BASE_MS", "250");
        let (buf, _guard) = capture_warnings();
        let c = ServerConfig::from_env();
        assert_eq!(c.webhook_backoff_base_ms, 250);
        // …and emits a deprecation warning naming the variable.
        let logged = buf.lock().unwrap().clone();
        assert!(
            logged.contains("PYLON_WEBHOOK_RETRY_BASE_MS"),
            "expected deprecation warning, got: {logged}"
        );
        std::env::remove_var("PYLON_WEBHOOK_RETRY_BASE_MS");

        // MAX_RETRIES is ignored (budget-bounded retries now) but still warns.
        std::env::set_var("PYLON_WEBHOOK_MAX_RETRIES", "7");
        let (buf, _guard) = capture_warnings();
        let c = ServerConfig::from_env();
        assert_eq!(
            c.webhook_retry_budget_ms, 300000,
            "MAX_RETRIES must not alter the budget"
        );
        let logged = buf.lock().unwrap().clone();
        assert!(
            logged.contains("PYLON_WEBHOOK_MAX_RETRIES"),
            "expected deprecation warning, got: {logged}"
        );
        std::env::remove_var("PYLON_WEBHOOK_MAX_RETRIES");

        // When both the alias and the canonical var are set, canonical wins.
        std::env::set_var("PYLON_WEBHOOK_RETRY_BASE_MS", "250");
        std::env::set_var("PYLON_WEBHOOK_BACKOFF_BASE_MS", "500");
        let c = ServerConfig::from_env();
        assert_eq!(c.webhook_backoff_base_ms, 500);
        std::env::remove_var("PYLON_WEBHOOK_RETRY_BASE_MS");
        std::env::remove_var("PYLON_WEBHOOK_BACKOFF_BASE_MS");
    }

    #[test]
    fn app_sweep_interval_parses_from_env() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("PYLON_APP_SWEEP_INTERVAL", "30");
        let c = ServerConfig::from_env();
        assert_eq!(c.app_sweep_interval_secs, 30);
        std::env::remove_var("PYLON_APP_SWEEP_INTERVAL");
    }

    #[test]
    fn app_sweep_interval_defaults_to_zero() {
        assert_eq!(ServerConfig::default().app_sweep_interval_secs, 0);
    }

    #[test]
    fn cluster_envelope_compat_env_overrides_apply() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Hermetic under any ambient environment (the cluster gate itself runs
        // with this var exported): clear it before asserting the default.
        std::env::remove_var("PYLON_CLUSTER_ENVELOPE_COMPAT");
        // Default (unset): compat double-carry ON.
        assert!(ServerConfig::from_env().cluster_envelope_compat);
        // The explicit opt-outs for a fleet of knob-shipping builds.
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "0");
        assert!(!ServerConfig::from_env().cluster_envelope_compat);
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "false");
        assert!(!ServerConfig::from_env().cluster_envelope_compat);
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "FALSE");
        assert!(!ServerConfig::from_env().cluster_envelope_compat);
        // `off` also opts out (the PYLON_APP_CACHE sibling parsing shape).
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "off");
        assert!(!ServerConfig::from_env().cluster_envelope_compat);
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "OFF");
        assert!(!ServerConfig::from_env().cluster_envelope_compat);
        // The explicit re-enables.
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "1");
        assert!(ServerConfig::from_env().cluster_envelope_compat);
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "true");
        assert!(ServerConfig::from_env().cluster_envelope_compat);
        std::env::remove_var("PYLON_CLUSTER_ENVELOPE_COMPAT");
    }
}
