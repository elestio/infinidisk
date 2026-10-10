use anyhow::Result;
use infinidisk2::config::Config;
use std::process::Command;

#[test]
fn generated_profile_is_explicit_and_old_configs_keep_their_contract() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("config.toml");
    let executable = env!("CARGO_BIN_EXE_infinidisk2");
    assert!(
        Command::new(executable)
            .args(["-c", path.to_str().unwrap(), "config"])
            .status()?
            .success()
    );
    let chosen = Config::load(&path)?;
    assert!(chosen.adaptive_reads && chosen.logical_cache && chosen.wal_fixed_size);
    assert!(chosen.wal_commit_records && chosen.selective_sync && chosen.checkpoint_pipeline);
    assert!(!chosen.generation_mode && !chosen.aligned_wal && !chosen.compact_checkpoints);
    assert_eq!(chosen.segment_mib, 32);
    assert_eq!(chosen.remote_index_cache_mib, 128);
    let saved = std::fs::read(&path)?;
    assert!(
        !Command::new(executable)
            .args(["-c", path.to_str().unwrap(), "config"])
            .status()?
            .success()
    );
    assert_eq!(std::fs::read(&path)?, saved);
    let old: Config = toml::from_str("checkpoint_seconds = 5")?;
    assert!(!old.adaptive_reads && !old.wal_fixed_size && !old.generation_mode);
    assert_eq!(old.remote_index_cache_mib, 0);
    Ok(())
}
