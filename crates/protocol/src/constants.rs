pub const MAX_EVENT_BYTES: usize = 131_072;
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 131_072;
pub const MAX_INLINE_EVIDENCE_BYTES: usize = 65_536;
pub const MAX_INLINE_MEDIA_BYTES: usize = 0;
pub const MAX_STRUCTURED_SUMMARY_BYTES: usize = 65_536;
pub const CLOCK_SKEW_TOLERANCE_MS: u64 = 3_000;
pub const BACKFILL_BATCH_EVENTS: usize = 256;
pub const LOCAL_PROTOCOL_VERSION: &str = "0.1.0";
pub const REPUTATION_SCALE: i64 = 10_000;
pub const DEFAULT_SETTLEMENT_K: u32 = 50;

pub const INLINE_MIME_ALLOWLIST: &[&str] = &[
    "application/json",
    "text/plain",
    "text/markdown",
    "text/csv",
];
