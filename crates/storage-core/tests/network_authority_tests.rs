use serde_json::json;
use wattswarm_crypto::NodeIdentity;
use wattswarm_crypto::sha256_hex;
use wattswarm_storage_core::types::{
    AuthoritySet, AuthoritySignature, DEFAULT_CONTROL_RANGE_LIMIT, NetworkControlKind,
    NetworkControlRecord, NetworkProtocolParams, SignedNetworkProtocolParamsEnvelope,
    UnsignedNetworkProtocolParamsEnvelope,
};
use wattswarm_storage_core::{PgStore, network_control_payload_hash, network_control_record_hash};

fn signed_control_record(
    network_id: &str,
    signer: &NodeIdentity,
    control_seq: u64,
    prev_control_hash: Option<String>,
    kind: NetworkControlKind,
    payload: serde_json::Value,
) -> NetworkControlRecord {
    let payload_hash = network_control_payload_hash(&payload).expect("payload hash");
    let mut record = NetworkControlRecord {
        network_id: network_id.to_owned(),
        control_seq,
        prev_control_hash,
        control_hash: String::new(),
        kind,
        payload_hash,
        payload,
        authority_set_id: 0,
        signatures: Vec::new(),
        created_at: 100,
        activation: None,
    };
    record.control_hash = network_control_record_hash(&record).expect("record hash");
    record.signatures.push(AuthoritySignature {
        signer_node_id: signer.node_id(),
        signature_hex: signer.sign_bytes(record.control_hash.as_bytes()),
    });
    record
}

#[test]
fn bootstrap_network_does_not_initialize_authority_set_for_regular_sync() {
    let genesis = NodeIdentity::from_seed([21_u8; 32]);
    let network_id = "mainnet-authority-no-auto-init";
    let store = PgStore::open_in_memory().expect("store");

    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Authority No Auto Init",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");

    store
        .load_latest_network_authority_set(network_id)
        .expect_err("authority set is not initialized by topology/bootstrap sync");
}

#[test]
fn imported_bootstrap_bundle_does_not_initialize_authority_set() {
    let genesis = NodeIdentity::from_seed([26_u8; 32]);
    let network_id = "mainnet-authority-import-no-auto-init";
    let source = PgStore::open_in_memory().expect("source store");
    source
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Authority Import No Auto Init",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("source topology");
    source
        .put_network_protocol_params(network_id, &genesis, &NetworkProtocolParams::default())
        .expect("signed params");
    let bundle = source
        .for_org(wattswarm_storage_core::bootstrap_org_id(network_id))
        .load_network_bootstrap_bundle()
        .expect("bundle");

    let joined = PgStore::open_in_memory().expect("joined store");
    joined
        .import_network_bootstrap_bundle(&bundle)
        .expect("import bundle");
    joined
        .load_latest_network_authority_set(network_id)
        .expect_err("ordinary bootstrap import does not initialize authority set");
}

#[test]
fn signed_network_params_use_kv_payload_and_preserve_unknown_keys() {
    let genesis = NodeIdentity::from_seed([27_u8; 32]);
    let network_id = "mainnet-params-kv";
    let store = PgStore::open_in_memory().expect("store");
    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Params Kv",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");

    let mut params_kv = NetworkProtocolParams::default().to_kv_map();
    params_kv.insert("future_window_size".to_owned(), json!(42));
    params_kv.insert("max_established_per_peer".to_owned(), json!(99));
    params_kv.insert("gossip_mesh_d".to_owned(), json!(8));

    let signed = store
        .put_network_protocol_params_kv(network_id, &genesis, &params_kv)
        .expect("signed kv params");
    let signed_kv = signed.params_kv.as_ref().expect("kv payload");
    assert_eq!(signed_kv.get("future_window_size"), Some(&json!(42)));
    assert!(!signed_kv.contains_key("max_established_per_peer"));
    assert_eq!(signed.params.gossip_mesh_d, 8);
    assert_eq!(
        signed.params.max_established_per_peer,
        NetworkProtocolParams::default().max_established_per_peer
    );
    store
        .validate_signed_network_protocol_params(network_id, &signed)
        .expect("kv envelope verifies");
}

#[test]
fn signed_network_params_reject_noncanonical_kv_payload() {
    let genesis = NodeIdentity::from_seed([29_u8; 32]);
    let network_id = "mainnet-params-noncanonical";
    let store = PgStore::open_in_memory().expect("store");
    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Params Noncanonical",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");
    let mut signed = store
        .put_network_protocol_params(network_id, &genesis, &NetworkProtocolParams::default())
        .expect("signed params");
    signed
        .params_kv
        .as_mut()
        .expect("kv payload")
        .insert("max_established_per_peer".to_owned(), json!(99));
    let err = store
        .validate_signed_network_protocol_params(network_id, &signed)
        .expect_err("noncanonical kv is rejected");
    assert!(
        err.to_string()
            .contains("network params kv payload is not canonical")
    );
}

#[test]
fn legacy_signed_network_params_without_kv_still_verify() {
    let genesis = NodeIdentity::from_seed([28_u8; 32]);
    let network_id = "mainnet-params-legacy";
    let store = PgStore::open_in_memory().expect("store");
    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Params Legacy",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");
    let params = NetworkProtocolParams {
        gossip_mesh_d_high: 14,
        ..NetworkProtocolParams::default()
    };
    let unsigned = UnsignedNetworkProtocolParamsEnvelope {
        network_id: network_id.to_owned(),
        version: 1,
        prev_hash: None,
        params: params.clone(),
    };
    let params_hash = sha256_hex(&serde_json::to_vec(&unsigned).expect("legacy payload json"));
    let signed = SignedNetworkProtocolParamsEnvelope {
        network_id: network_id.to_owned(),
        version: 1,
        prev_hash: None,
        params_hash: params_hash.clone(),
        params_kv: None,
        params,
        signed_by: genesis.node_id(),
        signature: genesis.sign_bytes(params_hash.as_bytes()),
    };
    store
        .validate_signed_network_protocol_params(network_id, &signed)
        .expect("legacy envelope verifies");
}

#[test]
fn genesis_node_updates_signed_authority_set() {
    let genesis = NodeIdentity::from_seed([31_u8; 32]);
    let network_id = "mainnet-authority-signed-update";
    let store = PgStore::open_in_memory().expect("store");

    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Authority Signed Update",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");

    let authority_set = AuthoritySet::genesis(&genesis.node_id());
    let signed = store
        .put_network_authority_set(network_id, &genesis, &authority_set)
        .expect("genesis signs authority set");
    assert_eq!(signed.network_id, network_id);
    assert_eq!(signed.authority_set.authority_set_id, 0);
    assert_eq!(signed.authority_set.members[0].node_id, genesis.node_id());
    assert_eq!(signed.signed_by, genesis.node_id());
    assert!(signed.prev_hash.is_none());

    let loaded = store
        .load_latest_signed_network_authority_set(network_id)
        .expect("signed authority set");
    assert_eq!(loaded, signed);
    assert_eq!(
        store
            .load_latest_network_authority_set(network_id)
            .expect("authority set"),
        authority_set
    );
}

#[test]
fn non_genesis_node_cannot_update_authority_set() {
    let genesis = NodeIdentity::from_seed([32_u8; 32]);
    let outsider = NodeIdentity::from_seed([33_u8; 32]);
    let network_id = "mainnet-authority-non-genesis";
    let store = PgStore::open_in_memory().expect("store");

    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Authority Non Genesis",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");

    let err = store
        .put_network_authority_set(
            network_id,
            &outsider,
            &AuthoritySet::genesis(&genesis.node_id()),
        )
        .expect_err("non-genesis signer is rejected");
    assert!(
        err.to_string()
            .contains("network authority set update must be signed by genesis node")
    );
}

#[test]
fn network_control_record_requires_current_authority_signature() {
    let genesis = NodeIdentity::from_seed([22_u8; 32]);
    let outsider = NodeIdentity::from_seed([23_u8; 32]);
    let network_id = "mainnet-authority-control";
    let store = PgStore::open_in_memory().expect("store");

    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Authority Control",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");
    store
        .put_network_authority_set(
            network_id,
            &genesis,
            &AuthoritySet::genesis(&genesis.node_id()),
        )
        .expect("authority set");

    let valid = signed_control_record(
        network_id,
        &genesis,
        1,
        None,
        NetworkControlKind::SoftwareSignal,
        json!({"protocol_version": "/wattswarm/0.1.0"}),
    );
    store
        .validate_network_control_record(&valid)
        .expect("genesis signature is accepted");
    store
        .append_network_control_record(&valid)
        .expect("valid record appends");

    let outsider_record = signed_control_record(
        network_id,
        &outsider,
        1,
        None,
        NetworkControlKind::SoftwareSignal,
        json!({"protocol_version": "/wattswarm/0.1.0"}),
    );
    let err = store
        .validate_network_control_record(&outsider_record)
        .expect_err("outsider signature is rejected");
    assert!(
        err.to_string()
            .contains("network control authority signatures insufficient")
    );
}

#[test]
fn network_control_log_requires_contiguous_hash_chain() {
    let genesis = NodeIdentity::from_seed([24_u8; 32]);
    let network_id = "mainnet-authority-chain";
    let store = PgStore::open_in_memory().expect("store");

    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Authority Chain",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");
    store
        .put_network_authority_set(
            network_id,
            &genesis,
            &AuthoritySet::genesis(&genesis.node_id()),
        )
        .expect("authority set");

    let first = signed_control_record(
        network_id,
        &genesis,
        1,
        None,
        NetworkControlKind::SoftwareSignal,
        json!({"protocol_version": "/wattswarm/0.1.0"}),
    );
    store
        .append_network_control_record(&first)
        .expect("first record");

    let skipped = signed_control_record(
        network_id,
        &genesis,
        3,
        Some(first.control_hash.clone()),
        NetworkControlKind::SoftwareSignal,
        json!({"protocol_version": "/wattswarm/0.1.1"}),
    );
    let err = store
        .append_network_control_record(&skipped)
        .expect_err("skipped seq rejected");
    assert!(
        err.to_string()
            .contains("network control record must append at next seq")
    );

    let second = signed_control_record(
        network_id,
        &genesis,
        2,
        Some(first.control_hash.clone()),
        NetworkControlKind::SoftwareSignal,
        json!({"protocol_version": "/wattswarm/0.1.1"}),
    );
    store
        .append_network_control_record(&second)
        .expect("second record");
    assert_eq!(
        store.network_control_head(network_id).expect("head"),
        (2, Some(second.control_hash.clone()))
    );
    let range = store
        .load_network_control_range(network_id, 1, 10)
        .expect("range");
    assert_eq!(range.len(), 2);
    assert_eq!(range[0].control_hash, first.control_hash);
    assert_eq!(range[1].control_hash, second.control_hash);
}

#[test]
fn network_control_range_is_capped_to_default_limit() {
    let genesis = NodeIdentity::from_seed([25_u8; 32]);
    let network_id = "mainnet-authority-range-cap";
    let store = PgStore::open_in_memory().expect("store");

    store
        .ensure_mainnet_bootstrap_network_topology(
            network_id,
            "Authority Range Cap",
            &genesis.node_id(),
            &genesis.node_id(),
            100,
        )
        .expect("topology");
    store
        .put_network_authority_set(
            network_id,
            &genesis,
            &AuthoritySet::genesis(&genesis.node_id()),
        )
        .expect("authority set");

    let mut prev = None;
    for seq in 1..=(DEFAULT_CONTROL_RANGE_LIMIT as u64 + 1) {
        let record = signed_control_record(
            network_id,
            &genesis,
            seq,
            prev,
            NetworkControlKind::SoftwareSignal,
            json!({"protocol_version": format!("/wattswarm/0.1.{seq}")}),
        );
        prev = Some(record.control_hash.clone());
        store
            .append_network_control_record(&record)
            .expect("append record");
    }

    let range = store
        .load_network_control_range(network_id, 1, DEFAULT_CONTROL_RANGE_LIMIT + 100)
        .expect("range");
    assert_eq!(range.len(), DEFAULT_CONTROL_RANGE_LIMIT);
    assert_eq!(range[0].control_seq, 1);
    assert_eq!(range[DEFAULT_CONTROL_RANGE_LIMIT - 1].control_seq, 256);
}
