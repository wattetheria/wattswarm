use anyhow::{Context, Result, bail};
use fs2::FileExt;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

const MAINTENANCE_LOCK_FILE: &str = ".wattswarm-node-maintenance.lock";
const MAINTENANCE_TICK: Duration = Duration::from_millis(50);
static ACTIVE_MAINTENANCE_OWNERS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

pub fn start_node_maintenance_once(state_dir: PathBuf, db_path: PathBuf) -> Result<bool> {
    let canonical_state_dir = state_dir
        .canonicalize()
        .unwrap_or_else(|_| state_dir.clone());
    let mut active = ACTIVE_MAINTENANCE_OWNERS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if active.contains(&canonical_state_dir) {
        return Ok(false);
    }
    let lock = acquire_owner_lock(&state_dir)?;
    active.insert(canonical_state_dir.clone());
    drop(active);
    thread::spawn(move || {
        let _lock = lock;
        let result = run_node_maintenance_loop(&state_dir, &db_path);
        ACTIVE_MAINTENANCE_OWNERS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&canonical_state_dir);
        if let Err(error) = result {
            eprintln!("wattswarm node maintenance stopped: {error:#}");
        }
    });
    Ok(true)
}

fn acquire_owner_lock(state_dir: &Path) -> Result<File> {
    std::fs::create_dir_all(state_dir)?;
    let path = state_dir.join(MAINTENANCE_LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("open node maintenance owner lock {}", path.display()))?;
    file.try_lock_exclusive().map_err(|error| {
        anyhow::anyhow!(
            "another process owns node maintenance for {}: {error}",
            state_dir.display()
        )
    })?;
    Ok(file)
}

fn run_node_maintenance_loop(state_dir: &Path, db_path: &Path) -> Result<()> {
    let mut node = crate::control::open_configured_node(state_dir, db_path)
        .context("open node for maintenance")?;
    let agent_inbox = crate::network_bridge::AgentInboxWorker::start();
    loop {
        crate::network_hooks::run_background_post_tick(&mut node, state_dir);
        let _ = agent_inbox.scan_once(state_dir, db_path);
        thread::sleep(MAINTENANCE_TICK);
    }
}

pub fn assert_no_maintenance_for_local_mode() -> Result<()> {
    if crate::control::network_service::node_maintenance_explicitly_enabled() {
        bail!("pure Local mode cannot enable WATTSWARM_NODE_MAINTENANCE_ENABLED in V1");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn owner_lock_rejects_a_second_process_handle() {
        let dir = tempdir().unwrap();
        let first = acquire_owner_lock(dir.path()).unwrap();
        assert!(acquire_owner_lock(dir.path()).is_err());
        drop(first);
        assert!(acquire_owner_lock(dir.path()).is_ok());
    }
}
