//! WATCH TOGETHER connector — Rust. Client for the SoriTrading agent's local API:
//! WATCH TOGETHER signals and exchange rate-limit coordination. Read-only: no order, key or balance operation exists.

pub mod client;
pub mod control;
pub mod sse;

pub use client::{ApiError, Client, LevelEvent, Options, Uses, OPS, PROTOCOL_VERSION};
pub use control::{Ack, ControlTracker, Notice, ANY};
pub use sse::{SseEvent, SseParser};
