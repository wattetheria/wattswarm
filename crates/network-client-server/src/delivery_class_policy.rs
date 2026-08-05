use wattswarm_network_transport_core::{DeliveryClass, PropagationLane};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventDeliveryUrgency {
    ExplicitRecipient,
    TimeSensitiveControl,
    Background,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryClassInput {
    pub lane: PropagationLane,
    pub event_urgency: EventDeliveryUrgency,
}

pub fn delivery_class_for_record(input: DeliveryClassInput) -> DeliveryClass {
    match input.lane {
        PropagationLane::Messages | PropagationLane::Rules => DeliveryClass::Interactive,
        PropagationLane::Checkpoints | PropagationLane::Summaries => DeliveryClass::Bulk,
        PropagationLane::Events => match input.event_urgency {
            EventDeliveryUrgency::ExplicitRecipient
            | EventDeliveryUrgency::TimeSensitiveControl => DeliveryClass::Interactive,
            EventDeliveryUrgency::Background => DeliveryClass::Bulk,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_v1_policy_covers_all_lanes() {
        let cases = [
            (PropagationLane::Messages, DeliveryClass::Interactive),
            (PropagationLane::Rules, DeliveryClass::Interactive),
            (PropagationLane::Checkpoints, DeliveryClass::Bulk),
            (PropagationLane::Summaries, DeliveryClass::Bulk),
        ];
        for (lane, expected) in cases {
            assert_eq!(
                delivery_class_for_record(DeliveryClassInput {
                    lane,
                    event_urgency: EventDeliveryUrgency::Background,
                }),
                expected
            );
        }
        assert_eq!(
            delivery_class_for_record(DeliveryClassInput {
                lane: PropagationLane::Events,
                event_urgency: EventDeliveryUrgency::ExplicitRecipient,
            }),
            DeliveryClass::Interactive
        );
        assert_eq!(
            delivery_class_for_record(DeliveryClassInput {
                lane: PropagationLane::Events,
                event_urgency: EventDeliveryUrgency::Background,
            }),
            DeliveryClass::Bulk
        );
    }
}
