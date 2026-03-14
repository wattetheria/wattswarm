use super::*;

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

pub(super) fn dynamic_subscription_scopes_for_node(
    node: &Node,
    node_id: &str,
) -> Result<Vec<SwarmScope>> {
    let mut scopes = Vec::new();
    for hint in node
        .store
        .list_active_feed_subscription_scope_hints(node_id)?
    {
        if let Some(scope) = parse_scope_hint_string(&hint)
            && !scopes.contains(&scope)
        {
            scopes.push(scope);
        }
    }
    Ok(scopes)
}

pub(super) fn node_has_active_subscription_scope(
    node: &Node,
    node_id: &str,
    scope: &SwarmScope,
) -> Result<bool> {
    Ok(dynamic_subscription_scopes_for_node(node, node_id)?
        .into_iter()
        .any(|candidate| candidate == *scope))
}

pub(super) fn parse_scope_hint_string(raw: &str) -> Option<SwarmScope> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("global") {
        return Some(SwarmScope::Global);
    }
    let (kind, rest) = trimmed.split_once(':')?;
    let id = rest
        .split([':', '/'])
        .next()
        .map(str::trim)
        .unwrap_or_default();
    if id.is_empty() {
        return None;
    }
    match kind.trim().to_ascii_lowercase().as_str() {
        "region" => Some(SwarmScope::Region(id.to_owned())),
        "local" | "node" => Some(SwarmScope::Node(id.to_owned())),
        _ => None,
    }
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
    {
        let kind = obj.get("kind").and_then(serde_json::Value::as_str);
        let id = obj.get("id").and_then(serde_json::Value::as_str);
        match (
            kind.map(|v| v.trim().to_ascii_lowercase()),
            id.map(str::trim),
        ) {
            (Some(kind), Some(id)) if !id.is_empty() && kind == "region" => {
                return SwarmScope::Region(id.to_owned());
            }
            (Some(kind), Some(id))
                if !id.is_empty() && matches!(kind.as_str(), "local" | "node") =>
            {
                return SwarmScope::Node(id.to_owned());
            }
            (Some(kind), _) if kind == "global" => return SwarmScope::Global,
            _ => {}
        }
    }
    parse_scope_hint_string(&contract.task_type).unwrap_or(SwarmScope::Global)
}

pub(super) fn scope_from_hint_or_global(raw: &str) -> SwarmScope {
    parse_scope_hint_string(raw).unwrap_or(SwarmScope::Global)
}

pub(super) fn event_scope(node: &Node, event: &crate::types::Event) -> Result<SwarmScope> {
    match &event.payload {
        crate::types::EventPayload::TaskCreated(contract) => Ok(contract_scope(contract)),
        crate::types::EventPayload::FeedSubscriptionUpdated(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
        }
        crate::types::EventPayload::TaskAnnounced(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
        }
        crate::types::EventPayload::ExecutionIntentDeclared(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
        }
        crate::types::EventPayload::ExecutionSetConfirmed(payload) => {
            Ok(scope_from_hint_or_global(&payload.scope_hint))
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
