//! Per-core clustering glue.
//!
//! The percore worker (see [`crate::transport::worker`]) runs a SYNC mio loop and must
//! never block on Redis. So all Redis lives on a dedicated tokio runtime owned by the
//! [`ClusterBridge`]; the worker fires fire-and-forget commands to it over a bounded
//! control-plane channel and never awaits a reply.
//!
//! [`ClusterBridge`]: bridge::ClusterBridge

pub mod adapter;
pub mod bridge;
