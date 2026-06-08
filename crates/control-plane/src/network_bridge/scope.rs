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

fn scope_from_optional_hint(scope: Option<crate::types::ScopeHint>) -> SwarmScope {
    scope
        .map(scope_from_projection_scope)
        .unwrap_or(SwarmScope::Global)
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

pub(super) fn remote_feed_subscription_payloads_for_relay(
    node: &Node,
    local_node_id: &str,
) -> Result<Vec<crate::types::FeedSubscriptionUpdatedPayload>> {
    let network_id = super::current_network_context_id(node);
    let mut subscriptions = Vec::new();
    for subscription in node
        .store
        .list_active_feed_subscriptions_for_network(&network_id)?
    {
        if subscription.subscriber_node_id == local_node_id {
            continue;
        }
        if parse_scope_hint_string(&subscription.scope_hint).is_none() {
            continue;
        }
        subscriptions.push(crate::types::FeedSubscriptionUpdatedPayload {
            network_id: subscription.network_id,
            subscriber_node_id: subscription.subscriber_node_id,
            feed_key: subscription.feed_key,
            scope_hint: subscription.scope_hint,
            gossip_kinds: subscription.gossip_kinds,
            provider_capabilities: subscription.provider_capabilities,
            agent_envelope: None,
            active: true,
        });
    }
    Ok(subscriptions)
}

pub(super) fn feed_subscription_target_scope(
    payload: &crate::types::FeedSubscriptionUpdatedPayload,
) -> SwarmScope {
    scope_from_optional_hint(payload.scope())
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

pub(super) fn event_scope(node: &Node, event: &crate::types::Event) -> Result<SwarmScope> {
    match &event.payload {
        crate::types::EventPayload::TaskCreated(contract) => Ok(contract_scope(contract)),
        crate::types::EventPayload::FeedSubscriptionUpdated(_) => Ok(SwarmScope::Global),
        crate::types::EventPayload::TaskAnnounced(payload) => {
            Ok(scope_from_optional_hint(payload.scope()))
        }
        crate::types::EventPayload::ExecutionIntentDeclared(payload) => {
            Ok(scope_from_optional_hint(payload.scope()))
        }
        crate::types::EventPayload::ExecutionSetConfirmed(payload) => {
            Ok(scope_from_optional_hint(payload.scope()))
        }
        crate::types::EventPayload::TopicMessagePosted(payload) => {
            Ok(scope_from_optional_hint(payload.scope()))
        }
        crate::types::EventPayload::AgentPaymentPosted(payload) => {
            Ok(SwarmScope::Node(payload.remote_node_id.clone()))
        }
        crate::types::EventPayload::MembershipUpdated(_)
        | crate::types::EventPayload::PolicyTuned(_)
        | crate::types::EventPayload::CheckpointCreated(_)
        | crate::types::EventPayload::AdvisoryCreated(_)
        | crate::types::EventPayload::AdvisoryApproved(_)
        | crate::types::EventPayload::AdvisoryApplied(_)
        | crate::types::EventPayload::EventRevoked(_)
        | crate::types::EventPayload::SummaryRevoked(_)
        | crate::types::EventPayload::NodePenalized(_) => Ok(SwarmScope::Global),
        _ => {
            if let Some(task_id) = event.task_id.as_deref()
                && let Some(task) = node.task_view(task_id)?
            {
                return Ok(contract_scope(&task.contract));
            }
            Ok(SwarmScope::Global)
        }
    }
}
