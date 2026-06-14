use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoundPolicy {
    pub min_participants: usize,
    pub threshold_percent: u32,
    pub round_timeout_ms: u64,
    pub max_rounds: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_decision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoundState {
    pub round_index: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_opened_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundDecision {
    ThresholdMet,
    OpenNextRound,
    FallbackFinalize,
    KeepWaiting,
}

impl RoundPolicy {
    pub fn normalized(self) -> Self {
        Self {
            min_participants: self.min_participants.max(1),
            threshold_percent: self.threshold_percent.clamp(1, 100),
            round_timeout_ms: self.round_timeout_ms,
            max_rounds: self.max_rounds.max(1),
            fallback_decision: self.fallback_decision.and_then(|value| {
                let trimmed = value.trim();
                matches!(trimmed, "support" | "reject" | "abstain").then(|| trimmed.to_owned())
            }),
        }
    }
}

impl RoundState {
    pub fn initial() -> Self {
        Self {
            round_index: 1,
            round_opened_at_ms: None,
        }
    }
}

pub fn threshold_count(total: usize, threshold_percent: u32) -> usize {
    if total == 0 {
        return 0;
    }
    let total = total as u64;
    let threshold = threshold_percent.clamp(1, 100) as u64;
    total.saturating_mul(threshold).div_ceil(100) as usize
}

pub fn required_count_for_observed(
    policy: &RoundPolicy,
    observed_participant_count: usize,
) -> usize {
    threshold_count(
        policy.min_participants.max(observed_participant_count),
        policy.threshold_percent,
    )
}

pub fn evaluate_round_policy(
    policy: &RoundPolicy,
    state: &RoundState,
    observed_participant_count: usize,
    winning_count: usize,
    now_ms: u64,
) -> RoundDecision {
    let required_count = required_count_for_observed(policy, observed_participant_count);
    if required_count > 0 && winning_count >= required_count {
        return RoundDecision::ThresholdMet;
    }

    let Some(round_opened_at_ms) = state.round_opened_at_ms else {
        return RoundDecision::KeepWaiting;
    };
    if policy.round_timeout_ms == 0
        || now_ms < round_opened_at_ms.saturating_add(policy.round_timeout_ms)
    {
        return RoundDecision::KeepWaiting;
    }

    if state.round_index < policy.max_rounds {
        RoundDecision::OpenNextRound
    } else {
        RoundDecision::FallbackFinalize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RoundPolicy {
        RoundPolicy {
            min_participants: 3,
            threshold_percent: 60,
            round_timeout_ms: 1_000,
            max_rounds: 2,
            fallback_decision: Some("abstain".to_owned()),
        }
    }

    #[test]
    fn threshold_count_uses_ceiling_division() {
        assert_eq!(threshold_count(3, 60), 2);
        assert_eq!(threshold_count(5, 60), 3);
        assert_eq!(threshold_count(1, 100), 1);
    }

    #[test]
    fn required_count_uses_minimum_denominator() {
        let policy = policy();
        assert_eq!(required_count_for_observed(&policy, 1), 2);
        assert_eq!(required_count_for_observed(&policy, 5), 3);
    }

    #[test]
    fn evaluate_round_policy_threshold_wins_before_timeout() {
        let policy = policy();
        let state = RoundState {
            round_index: 1,
            round_opened_at_ms: Some(100),
        };
        assert_eq!(
            evaluate_round_policy(&policy, &state, 3, 2, 200),
            RoundDecision::ThresholdMet
        );
    }

    #[test]
    fn evaluate_round_policy_opens_next_round_until_max_rounds() {
        let policy = policy();
        let state = RoundState {
            round_index: 1,
            round_opened_at_ms: Some(100),
        };
        assert_eq!(
            evaluate_round_policy(&policy, &state, 2, 1, 1_100),
            RoundDecision::OpenNextRound
        );
    }

    #[test]
    fn evaluate_round_policy_fallbacks_at_max_rounds() {
        let policy = policy();
        let state = RoundState {
            round_index: 2,
            round_opened_at_ms: Some(100),
        };
        assert_eq!(
            evaluate_round_policy(&policy, &state, 2, 1, 1_100),
            RoundDecision::FallbackFinalize
        );
    }
}
