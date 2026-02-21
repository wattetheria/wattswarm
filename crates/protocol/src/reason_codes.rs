pub const REASON_UNKNOWN: u16 = 0;
pub const REASON_SCHEMA_OK: u16 = 100;
pub const REASON_SCHEMA_INVALID: u16 = 101;
pub const REASON_CONFIDENCE_TOO_LOW: u16 = 102;
pub const REASON_OUTPUT_TOO_LARGE: u16 = 103;
pub const REASON_CHECK_SUMMARY_MISSING: u16 = 104;
pub const REASON_SCORE_TOO_LOW: u16 = 105;
pub const REASON_EVIDENCE_UNREACHABLE: u16 = 200;
pub const REASON_EVIDENCE_TIMEOUT: u16 = 201;
pub const REASON_EVIDENCE_AUTH_DENIED: u16 = 202;
pub const REASON_EVIDENCE_DIGEST_MISMATCH: u16 = 203;
pub const REASON_TASK_TIMEOUT: u16 = 300;
pub const REASON_INVALID_OUTPUT: u16 = 301;
pub const REASON_TASK_EXPIRED: u16 = 302;
pub const REASON_RUNTIME_CRASH: u16 = 303;
pub const REASON_CUSTOM_ERROR: u16 = 999;

pub const REASON_CODE_TABLE: &[(u16, &str, &str)] = &[
    (REASON_UNKNOWN, "UNKNOWN", "Unknown reason code"),
    (
        REASON_SCHEMA_OK,
        "SCHEMA_OK",
        "Output matches schema and policy checks",
    ),
    (
        REASON_SCHEMA_INVALID,
        "SCHEMA_INVALID",
        "Output schema validation failed",
    ),
    (
        REASON_CONFIDENCE_TOO_LOW,
        "CONFIDENCE_TOO_LOW",
        "Candidate confidence below threshold",
    ),
    (
        REASON_OUTPUT_TOO_LARGE,
        "OUTPUT_TOO_LARGE",
        "Candidate output larger than threshold",
    ),
    (
        REASON_CHECK_SUMMARY_MISSING,
        "CHECK_SUMMARY_MISSING",
        "Cross-check summary is missing",
    ),
    (
        REASON_SCORE_TOO_LOW,
        "SCORE_TOO_LOW",
        "Cross-check score below threshold",
    ),
    (
        REASON_EVIDENCE_UNREACHABLE,
        "EVIDENCE_UNREACHABLE",
        "Verifier cannot reach external evidence source",
    ),
    (
        REASON_EVIDENCE_TIMEOUT,
        "EVIDENCE_TIMEOUT",
        "Verifier timed out while fetching external evidence",
    ),
    (
        REASON_EVIDENCE_AUTH_DENIED,
        "EVIDENCE_AUTH_DENIED",
        "Verifier denied by evidence source auth policy",
    ),
    (
        REASON_EVIDENCE_DIGEST_MISMATCH,
        "EVIDENCE_DIGEST_MISMATCH",
        "Fetched evidence digest does not match artifact_ref digest",
    ),
    (
        REASON_TASK_TIMEOUT,
        "TASK_TIMEOUT",
        "Runtime execution or verification timeout",
    ),
    (
        REASON_INVALID_OUTPUT,
        "INVALID_OUTPUT",
        "Runtime produced invalid output",
    ),
    (
        REASON_TASK_EXPIRED,
        "TASK_EXPIRED",
        "Task reached expiry before finalization",
    ),
    (
        REASON_RUNTIME_CRASH,
        "RUNTIME_CRASH",
        "Runtime process crashed or failed unexpectedly",
    ),
    (
        REASON_CUSTOM_ERROR,
        "CUSTOM_ERROR",
        "Runtime-specific custom error mapping",
    ),
];

pub fn reason_code_name(code: u16) -> &'static str {
    REASON_CODE_TABLE
        .iter()
        .find_map(|(c, name, _)| if *c == code { Some(*name) } else { None })
        .unwrap_or("UNKNOWN")
}

pub fn is_protocol_reason_code(code: u16) -> bool {
    REASON_CODE_TABLE.iter().any(|(c, _, _)| *c == code)
}
