use std::process::Command;

#[test]
fn warm_cli_rejects_out_of_range_concurrency_before_opening_config() {
    for value in ["0", "129", "65536", "-1"] {
        let output = Command::new(env!("CARGO_BIN_EXE_infinidisk2"))
            .args([
                "-c",
                "/nonexistent-infinidisk2-warm-test.toml",
                "warm",
                "--concurrency",
                value,
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("read config"));
    }
    for value in ["1", "32", "128"] {
        let output = Command::new(env!("CARGO_BIN_EXE_infinidisk2"))
            .args([
                "-c",
                "/nonexistent-infinidisk2-warm-test.toml",
                "warm",
                "--concurrency",
                value,
            ])
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&output.stderr).contains("read config"));
    }
}
