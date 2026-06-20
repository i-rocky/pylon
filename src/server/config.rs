//! Server-level configuration (defaults + `PYLON_*` env overrides).

/// Which app-manager backend to use for app lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum AppManagerKind {
    StaticFile,
    Sqlite,
}

/// Parse `PYLON_APP_MANAGER` into an [`AppManagerKind`].
/// Unknown values fall back to `StaticFile` (safe default).
pub fn parse_app_manager(kind: Option<&str>) -> AppManagerKind {
    match kind.unwrap_or("static") {
        "sqlite" => AppManagerKind::Sqlite,
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
    pub webhook_max_retries: u32,
    pub webhook_retry_base_ms: u64,
    pub webhook_max_concurrency: usize,
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
    /// drain before force-closing. After `shutdown` is set workers queue a WS
    /// Close(1001) on every open connection, then keep flushing until
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
    /// Which app-manager backend to use. `PYLON_APP_MANAGER` (`static` | `sqlite`;
    /// default `static`). Set to `sqlite` and provide `PYLON_APP_DSN`.
    pub app_manager: AppManagerKind,
    /// DSN for the SQLite app-manager. Required when `app_manager == Sqlite`.
    /// `PYLON_APP_DSN` (e.g. `sqlite:///var/lib/pylon/apps.db`).
    pub app_dsn: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".into(),
            port: 7000,
            activity_timeout: 120,
            pong_timeout: 30,
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
            webhook_max_retries: 3,
            webhook_retry_base_ms: 100,
            webhook_max_concurrency: 100,
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
        if let Ok(v) = std::env::var("PYLON_STRICT_PROTOCOL") {
            c.strict_protocol = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("PYLON_APPS_PATH") {
            c.apps_path = v;
        }
        c.app_manager = parse_app_manager(std::env::var("PYLON_APP_MANAGER").ok().as_deref());
        c.app_dsn = std::env::var("PYLON_APP_DSN").ok();
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
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_MAX_RETRIES") {
            if let Ok(p) = v.parse() {
                c.webhook_max_retries = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_RETRY_BASE_MS") {
            if let Ok(p) = v.parse() {
                c.webhook_retry_base_ms = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_WEBHOOK_MAX_CONCURRENCY") {
            if let Ok(p) = v.parse() {
                c.webhook_max_concurrency = p;
            }
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
    ///   [`resolved_memory_budget`]) so this method is pure.
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
        // webhook tunables (spec §6)
        assert_eq!(c.webhook_batch_ms, 50);
        assert_eq!(c.webhook_timeout_ms, 5000);
        assert_eq!(c.webhook_max_retries, 3);
        assert_eq!(c.webhook_retry_base_ms, 100);
        assert_eq!(c.webhook_max_concurrency, 100);
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
        std::env::set_var("PYLON_WORKERS", "3");
        let c = ServerConfig::from_env();
        assert_eq!(c.workers, 3);
        assert_eq!(c.worker_count(), 3);
        std::env::remove_var("PYLON_WORKERS");
    }

    #[test]
    fn parse_app_manager_maps_kinds() {
        assert_eq!(parse_app_manager(None), AppManagerKind::StaticFile);
        assert_eq!(parse_app_manager(Some("static")), AppManagerKind::StaticFile);
        assert_eq!(parse_app_manager(Some("sqlite")), AppManagerKind::Sqlite);
        assert_eq!(parse_app_manager(Some("weird")), AppManagerKind::StaticFile);
    }

    #[test]
    fn tls_env_overrides_apply() {
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

    #[test]
    fn webhook_env_overrides_apply() {
        // Use a guarded set/remove to avoid cross-test env bleed.
        std::env::set_var("PYLON_WEBHOOK_BATCH_MS", "25");
        std::env::set_var("PYLON_WEBHOOK_TIMEOUT_MS", "1234");
        std::env::set_var("PYLON_WEBHOOK_MAX_RETRIES", "7");
        std::env::set_var("PYLON_WEBHOOK_RETRY_BASE_MS", "10");
        std::env::set_var("PYLON_WEBHOOK_MAX_CONCURRENCY", "5");
        let c = ServerConfig::from_env();
        assert_eq!(c.webhook_batch_ms, 25);
        assert_eq!(c.webhook_timeout_ms, 1234);
        assert_eq!(c.webhook_max_retries, 7);
        assert_eq!(c.webhook_retry_base_ms, 10);
        assert_eq!(c.webhook_max_concurrency, 5);
        std::env::remove_var("PYLON_WEBHOOK_BATCH_MS");
        std::env::remove_var("PYLON_WEBHOOK_TIMEOUT_MS");
        std::env::remove_var("PYLON_WEBHOOK_MAX_RETRIES");
        std::env::remove_var("PYLON_WEBHOOK_RETRY_BASE_MS");
        std::env::remove_var("PYLON_WEBHOOK_MAX_CONCURRENCY");
    }
}
