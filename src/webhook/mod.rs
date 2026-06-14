//! Webhooks (SP5): WS-lifecycle-driven, signed, batched HTTP notifications.
//!
//! `WebhookEvent` (the trigger) → `WebhookHandle` (cheap-clone mpsc sender) →
//! `WebhookDispatcher` (actor: window + coalesce + sign) → `WebhookTransport`.

pub mod batch;
pub mod event;
pub mod transport;
