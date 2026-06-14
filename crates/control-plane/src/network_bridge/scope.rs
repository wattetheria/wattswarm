use super::*;
use crate::storage::ProjectionScope;

fn scope_from_projection_scope(scope: ProjectionScope) -> SwarmScope {
    match scope {
        ProjectionScope::Global => SwarmScope::Global,
        ProjectionScope::Region(id) => SwarmScope::Region(id),
        ProjectionScope::Node(id) => SwarmScope::Node(id),
        ProjectionScope::Group(id) => SwarmScope::Group(id),
    }
}

pub(super) fn merge_scopes(scopes: impl IntoIterator<Item = SwarmScope>) -> Vec<SwarmScope> {
    let mut merged = Vec::new();
    for scope in scopes {
        if !merged.contains(&scope) {
            merged.push(scope);
        }
    }
    if !merged.contains(&SwarmScope::Global) {
        merged.insert(0, SwarmScope::Global);
    }
    merged
}

#[cfg(test)]
pub(super) fn dynamic_subscription_scopes_for_node(
    node: &Node,
    node_id: &str,
) -> Result<Vec<SwarmScope>> {
    let mut scopes = Vec::new();
    for (scope, _) in dynamic_subscription_scope_kinds_for_node(node, node_id)? {
        if !scopes.contains(&scope) {
            scopes.push(scope);
        }
    }
    Ok(scopes)
}

pub(super) fn dynamic_subscription_scope_kinds_for_node(
    node: &Node,
    node_id: &str,
) -> Result<Vec<(SwarmScope, Vec<GossipKind>)>> {
    let mut subscriptions = Vec::new();
    let network_id = super::current_network_context_id(node);
    for subscription in node
        .store
        .list_active_feed_subscriptions(&network_id, node_id)?
    {
        let Some(scope) = parse_scope_hint_string(&subscription.scope_hint) else {
            continue;
        };
        let kinds = super::feed_subscription_gossip_kinds(&subscription.gossip_kinds);
        subscriptions.push((scope, kinds));
    }
    Ok(subscriptions)
}

pub(super) fn feed_subscription_target_scope(
    payload: &crate::types::FeedSubscriptionUpdatedPayload,
) -> Option<SwarmScope> {
    payload.scope().map(scope_from_projection_scope)
}

pub(super) fn event_matches_signed_scope(event: &crate::types::Event, scope: &SwarmScope) -> bool {
    signed_event_scope(event)
        .as_ref()
        .is_some_and(|signed_scope| signed_scope == scope)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EventTransportRoute {
    pub(super) scope: SwarmScope,
    pub(super) address: String,
    pub(super) public_global_control: bool,
}

impl EventTransportRoute {
    fn new(
        scope: SwarmScope,
        event_kind: crate::types::EventKind,
        public_global_control: bool,
    ) -> Result<Self> {
        let event_kind_label = format!("{event_kind:?}");
        let address = format!("ws.{}.{}", scope.label()?, event_kind_label);
        Ok(Self {
            scope,
            address,
            public_global_control,
        })
    }
}

pub(super) fn node_has_active_subscription_scope_kinds(
    node: &Node,
    node_id: &str,
    scope: &SwarmScope,
    kinds: &[GossipKind],
) -> Result<bool> {
    Ok(dynamic_subscription_scope_kinds_for_node(node, node_id)?
        .into_iter()
        .any(|(candidate_scope, candidate_kinds)| {
            candidate_scope == *scope && candidate_kinds.iter().any(|kind| kinds.contains(kind))
        }))
}

pub(super) fn parse_scope_hint_string(raw: &str) -> Option<SwarmScope> {
    crate::types::ScopeHint::parse_with_prefix_fallback(raw).map(scope_from_projection_scope)
}

#[cfg(test)]
fn contract_scope_from_object(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<SwarmScope> {
    let kind = obj.get("kind").and_then(serde_json::Value::as_str)?;
    let id = obj
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    crate::types::ScopeHint::from_kind_id(kind, id).map(scope_from_projection_scope)
}

#[cfg(test)]
pub(super) fn contract_scope(contract: &crate::types::TaskContract) -> SwarmScope {
    if let Some(raw) = contract
        .inputs
        .get("swarm_scope")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_scope_hint_string)
    {
        return raw;
    }
    if let Some(obj) = contract
        .inputs
        .get("swarm_scope")
        .and_then(serde_json::Value::as_object)
        .and_then(contract_scope_from_object)
    {
        return obj;
    }
    parse_scope_hint_string(&contract.task_type).unwrap_or(SwarmScope::Global)
}

fn route_for_scope(
    scope: SwarmScope,
    event_kind: crate::types::EventKind,
    public_global_control: bool,
) -> Result<Option<EventTransportRoute>> {
    Ok(Some(EventTransportRoute::new(
        scope,
        event_kind,
        public_global_control,
    )?))
}

fn signed_event_scope(event: &crate::types::Event) -> Option<SwarmScope> {
    parse_scope_hint_string(&event.swarm_scope)
}

fn is_public_global_control_event(payload: &crate::types::EventPayload) -> bool {
    matches!(
        payload,
        crate::types::EventPayload::MembershipUpdated(_)
            | crate::types::EventPayload::PolicyTuned(_)
            | crate::types::EventPayload::NetworkParamsUpdated(_)
            | crate::types::EventPayload::CheckpointCreated(_)
            | crate::types::EventPayload::AdvisoryCreated(_)
            | crate::types::EventPayload::AdvisoryApproved(_)
            | crate::types::EventPayload::AdvisoryApplied(_)
            | crate::types::EventPayload::EventRevoked(_)
            | crate::types::EventPayload::SummaryRevoked(_)
            | crate::types::EventPayload::NodePenalized(_)
    )
}

pub(super) fn event_transport_route(
    _node: &Node,
    event: &crate::types::Event,
) -> Result<Option<EventTransportRoute>> {
    let event_kind = event.payload.kind();
    let Some(scope) = signed_event_scope(event) else {
        return Ok(None);
    };
    route_for_scope(
        scope,
        event_kind,
        is_public_global_control_event(&event.payload),
    )
}

#[cfg(test)]
pub(super) fn event_scope(node: &Node, event: &crate::types::Event) -> Result<SwarmScope> {
    event_transport_route(node, event)?
        .map(|route| route.scope)
        .ok_or_else(|| anyhow!("event has no transport route: {}", event.event_id))
}
