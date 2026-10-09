use crate::config::Config;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use fs2::FileExt as _;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path,
};
use sha2::{Digest, Sha256};
use std::{fs::OpenOptions, ops::Range, path::PathBuf, sync::Arc};

#[derive(Clone)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    prefix: String,
    root: Option<PathBuf>,
}
impl Store {
    pub fn new(c: &Config) -> Result<Self> {
        if let Some(dir) = c.store.strip_prefix("file://") {
            ensure!(dir.starts_with('/'), "file store must be absolute");
            std::fs::create_dir_all(dir)?;
            Ok(Self {
                inner: Arc::new(LocalFileSystem::new_with_prefix(dir)?),
                prefix: String::new(),
                root: Some(std::fs::canonicalize(dir)?),
            })
        } else {
            let rest = c
                .store
                .strip_prefix("s3://")
                .context("store must start with s3:// or file://")?;
            let (bucket, prefix) = rest
                .split_once('/')
                .context("S3 requires a dedicated nonempty prefix")?;
            ensure!(
                !bucket.is_empty() && !prefix.trim_matches('/').is_empty(),
                "empty S3 bucket/prefix"
            );
            let mut b = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_region(&c.region)
                .with_virtual_hosted_style_request(false);
            if let Some(e) = &c.endpoint {
                b = b.with_endpoint(e);
            }
            Ok(Self {
                inner: Arc::new(b.build()?),
                prefix: prefix.trim_matches('/').to_owned(),
                root: None,
            })
        }
    }
    pub fn path(&self, key: &str) -> Path {
        Path::from(if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        })
    }
    pub async fn get(&self, key: &str) -> Result<Bytes> {
        Ok(self.inner.get(&self.path(key)).await?.bytes().await?)
    }
    pub async fn range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        Ok(self.inner.get_range(&self.path(key), range).await?)
    }
    pub async fn immutable(&self, key: &str, bytes: Bytes) -> Result<()> {
        // UUID object names are never reused for different contents. Retrying an upload is idempotent.
        if let Some(root) = &self.root {
            let path = root.join(key);
            let parent = path.parent().unwrap();
            let mut missing = Vec::new();
            let mut dir = parent;
            while !dir.exists() {
                missing.push(dir.to_path_buf());
                dir = dir.parent().context("invalid store path")?;
            }
            for dir in missing.into_iter().rev() {
                std::fs::create_dir(&dir)?;
                crate::wal::sync_dir(dir.parent().unwrap())?;
            }
            crate::wal::atomic(&path, &bytes)?;
            return Ok(());
        }
        self.inner.put(&self.path(key), bytes.into()).await?;
        Ok(())
    }
    pub async fn head(&self) -> Result<Option<(Bytes, UpdateVersion)>> {
        if let Some(root) = &self.root {
            return match std::fs::read(root.join("HEAD")) {
                Ok(b) => {
                    let v = UpdateVersion {
                        e_tag: Some(hex::encode(Sha256::digest(&b))),
                        version: None,
                    };
                    Ok(Some((b.into(), v)))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            };
        }
        match self.inner.get(&self.path("HEAD")).await {
            Ok(r) => {
                let v = UpdateVersion {
                    e_tag: r.meta.e_tag.clone(),
                    version: r.meta.version.clone(),
                };
                Ok(Some((r.bytes().await?, v)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    pub async fn cas(&self, bytes: Bytes, version: Option<UpdateVersion>) -> Result<UpdateVersion> {
        if let Some(root) = &self.root {
            let f = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(root.join(".HEAD.lock"))?;
            f.lock_exclusive()?;
            let current = self.head().await?;
            match (version, current) {
                (None, None) => {}
                (Some(expected), Some((_, actual))) => ensure!(
                    expected.e_tag == actual.e_tag,
                    "local HEAD conditional update conflict"
                ),
                _ => anyhow::bail!("local HEAD conditional create/update conflict"),
            }
            crate::wal::atomic(&root.join("HEAD"), &bytes)?;
            return Ok(UpdateVersion {
                e_tag: Some(hex::encode(Sha256::digest(&bytes))),
                version: None,
            });
        }
        let r = self
            .inner
            .put_opts(
                &self.path("HEAD"),
                bytes.into(),
                PutOptions {
                    mode: version.map_or(PutMode::Create, PutMode::Update),
                    ..Default::default()
                },
            )
            .await?;
        Ok(UpdateVersion {
            e_tag: r.e_tag,
            version: r.version,
        })
    }
}
