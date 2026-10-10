use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use infinidisk2::{config::Config, engine::Engine, linux, nbd};
use std::{path::PathBuf, time::Duration};
use tokio::sync::watch;

#[derive(Parser)]
#[command(name = "infinidisk", version, about)]
struct Cli {
    #[arg(short, long, default_value = "infinidisk.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Write the recommended configuration (refuses to overwrite).
    Config {
        /// Generate the original conservative settings for compatibility tests.
        #[arg(long)]
        legacy: bool,
    },
    /// Create a brand-new unformatted block volume; never replaces remote data.
    Init {
        #[arg(long,value_parser=parse_size)]
        size: u64,
    },
    /// Restore the last remote checkpoint into a new local directory.
    Adopt {
        /// The previous writer must already be stopped/fenced.
        #[arg(long)]
        takeover: bool,
    },
    /// Serve the block device over loopback NBD.
    Serve,
    /// Offline: cache all allocated logical pages (must fit the SSD budget).
    Warm {
        /// Maximum concurrent physical ranges (including cache probes/fills).
        #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(1..=128))]
        concurrency: u16,
    },
    #[cfg(feature = "ublk")]
    /// Experimental direct userspace block transport.
    Ublk {
        #[arg(long)]
        id: i32,
        #[arg(long, default_value_t = 4)]
        queues: u16,
    },
    #[cfg(feature = "ublk")]
    /// Delete only the specified experimental ublk device after unmount.
    UblkDelete {
        #[arg(long)]
        id: i32,
    },
    /// Offline: rewrite current remote pages into logical address order.
    Compact,
    /// Inspect the committed remote HEAD without opening the volume for writing.
    Status,
    /// Verify every referenced remote page and all metadata checksums.
    Scrub,
    /// Offline orphan collection; default is a dry run. Stop the server first.
    Gc {
        #[arg(long)]
        apply: bool,
        #[arg(long, default_value_t = 86400)]
        min_age_seconds: u64,
    },
    /// Attach natively with multiple sockets; runs in foreground until detached.
    Attach {
        #[arg(long)]
        device: PathBuf,
        #[arg(long, default_value_t = 8)]
        connections: u8,
    },
    /// Detach a Linux NBD device (unmount the filesystem first).
    Detach {
        #[arg(long)]
        device: PathBuf,
    },
}
fn parse_size(s: &str) -> std::result::Result<u64, String> {
    let (n, m) = if let Some(n) = s.strip_suffix("GiB") {
        (n, 1024u64.pow(3))
    } else if let Some(n) = s.strip_suffix("MiB") {
        (n, 1024u64.pow(2))
    } else {
        (s, 1)
    };
    n.parse::<u64>()
        .map_err(|e| e.to_string())?
        .checked_mul(m)
        .ok_or_else(|| "size overflow".into())
}
#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "infinidisk=info,infinidisk2=info,libublk=warn".into()),
        )
        .init();
    let cli = Cli::parse();
    if let Command::Config { legacy } = &cli.command {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&cli.config)?;
        let config = if *legacy {
            Config::default()
        } else {
            Config::recommended()
        };
        f.write_all(toml::to_string_pretty(&config)?.as_bytes())?;
        f.sync_all()?;
        println!("Configuration: {}", cli.config.display());
        return Ok(());
    }
    let c = Config::load(&cli.config)?;
    match cli.command {
        Command::Init { size } => println!(
            "{}",
            serde_json::to_string_pretty(&Engine::init(&c, size).await?)?
        ),
        Command::Adopt { takeover } => println!(
            "{}",
            serde_json::to_string_pretty(&Engine::adopt(&c, takeover).await?)?
        ),
        #[cfg(feature = "ublk")]
        Command::Ublk { id, queues } => {
            let e = Engine::open(c).await?;
            let engine = e.clone();
            let handle = tokio::runtime::Handle::current();
            let background_engine = e.clone();
            let (stop, mut stopped) = watch::channel(false);
            let background = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(
                    background_engine.config.checkpoint_seconds,
                ));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _=stopped.changed()=>break,
                        _=interval.tick()=> {if let Err(err)=background_engine.checkpoint().await {tracing::error!(error=%err,"ublk checkpoint failed");} let status=serde_json::to_string(&background_engine.status().await).unwrap(); tracing::info!(%status,"volume status");}
                    }
                }
            });
            let result = tokio::task::spawn_blocking(move || {
                infinidisk2::ublk::serve(engine, handle, id, queues)
            })
            .await?;
            let _ = stop.send(true);
            background.await?;
            result?;
            // Checkpoint includes the durable barrier. The generation-mode
            // admission barrier cannot run here: its publisher has stopped.
            e.checkpoint().await?;
            let status = serde_json::to_string(&e.status().await)?;
            tracing::info!(%status, "volume final status");
        }
        #[cfg(feature = "ublk")]
        Command::UblkDelete { id } => {
            tokio::task::spawn_blocking(move || infinidisk2::ublk::delete(id)).await??;
        }
        Command::Warm { concurrency } => println!(
            "Warmed {} allocated pages",
            Engine::warm_with_concurrency(c, usize::from(concurrency)).await?
        ),
        Command::Compact => println!("Compacted {} allocated pages", Engine::compact(c).await?),
        Command::Status => println!(
            "{}",
            serde_json::to_string_pretty(&Engine::inspect(&c).await?)?
        ),
        Command::Scrub => {
            let (seq, pages) = Engine::verify_remote(&c).await?;
            println!("Verified remote sequence {seq}: {pages} allocated 4 KiB pages");
        }
        Command::Gc {
            apply,
            min_age_seconds,
        } => println!(
            "{}",
            serde_json::to_string_pretty(&Engine::gc(&c, apply, min_age_seconds).await?)?
        ),
        Command::Attach {
            device,
            connections,
        } => {
            tokio::task::spawn_blocking(move || linux::attach(&c, &device, connections)).await??;
        }
        Command::Detach { device } => {
            tokio::task::spawn_blocking(move || linux::detach(&device)).await??;
        }
        Command::Serve => {
            let e = Engine::open(c).await?;
            let (tx, rx) = watch::channel(false);
            let engine = e.clone();
            let mut bg_shutdown = rx.clone();
            let background = tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(engine.config.checkpoint_seconds));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _=bg_shutdown.changed()=>break,
                        _=interval.tick()=>{
                            if let Err(err)=engine.checkpoint().await {tracing::error!(error=%err,"S3 checkpoint failed; local WAL retained");}
                            let status=serde_json::to_string(&engine.status().await).unwrap();
                            tracing::info!(%status,"volume status");
                        }
                    }
                }
            });
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            let server = nbd::serve(e.clone(), rx);
            tokio::pin!(server);
            let result = tokio::select! {
                r=&mut server=>r,
                _=tokio::signal::ctrl_c()=>{let _=tx.send(true);server.await},
                _=term.recv()=>{let _=tx.send(true);server.await},
            };
            let _ = tx.send(true);
            // Do not silently abandon an in-progress checkpoint on orderly shutdown.
            let _ = tokio::time::timeout(Duration::from_secs(60), background).await;
            // Checkpoint flushes the WAL itself and can clear generation lag.
            // A normal generation FLUSH would wait for the stopped publisher.
            tokio::time::timeout(Duration::from_secs(60), e.checkpoint())
                .await
                .context("shutdown checkpoint timed out; local WAL retained")??;
            let status = serde_json::to_string(&e.status().await)?;
            tracing::info!(%status, "volume final status");
            result?;
        }
        Command::Config { .. } => unreachable!(),
    }
    Ok(())
}
