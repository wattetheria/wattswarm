use std::collections::HashSet;
use wattswarm::reason_codes::{
    REASON_CODE_TABLE, REASON_CUSTOM_ERROR, REASON_SCHEMA_INVALID, is_protocol_reason_code,
    reason_code_name,
};

#[test]
fn reason_code_name_resolves_known_and_unknown_codes() {
    assert_eq!(reason_code_name(REASON_SCHEMA_INVALID), "SCHEMA_INVALID");
    assert_eq!(reason_code_name(65_000), "UNKNOWN");
}

#[test]
fn protocol_reason_code_membership_is_correct() {
    assert!(is_protocol_reason_code(REASON_CUSTOM_ERROR));
    assert!(!is_protocol_reason_code(65_000));
}

#[test]
fn reason_code_table_has_unique_codes() {
    let mut seen = HashSet::new();
    for (code, _, _) in REASON_CODE_TABLE {
        assert!(seen.insert(*code), "duplicate reason code: {code}");
    }
}
