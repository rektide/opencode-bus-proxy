//! systemd transaction boundary. The barrier only orders notifications;
//! DumpFileDescriptorStore verifies each accepted name AND kernel identity.
use crate::{Result, systemd};
use bus_proxy_gateway::handover::{self, Controller};
use std::{
    collections::HashMap,
    ffi::OsStr,
    io::{self, Write},
    os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd},
    os::unix::fs::FileExt,
    path::Path,
    time::Duration,
};

pub(crate) struct Manager {
    pub unit: String,
    pub user: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    mode: u32,
    major: u32,
    minor: u32,
    inode: u64,
}

fn identity(fd: &OwnedFd) -> io::Result<Identity> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut stat) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(Identity {
        mode: stat.st_mode,
        major: libc::major(stat.st_dev),
        minor: libc::minor(stat.st_dev),
        inode: stat.st_ino,
    })
}

impl Manager {
    async fn bus(&self, args: &[&str]) -> Result<serde_json::Value> {
        let output = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::process::Command::new("busctl")
                .arg(if self.user { "--user" } else { "--system" })
                .arg("--json=short")
                .args(args)
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() {
            return Err("systemd manager query failed (requires busctl and descriptor-store inspection permission)".into());
        }
        Ok(serde_json::from_slice(&output.stdout)?)
    }
    async fn path(&self) -> Result<String> {
        let value = self
            .bus(&[
                "call",
                "org.freedesktop.systemd1",
                "/org/freedesktop/systemd1",
                "org.freedesktop.systemd1.Manager",
                "GetUnit",
                "s",
                &self.unit,
            ])
            .await?;
        Ok(value["data"][0]
            .as_str()
            .ok_or("invalid systemd unit response")?
            .into())
    }
    async fn property(&self, path: &str, name: &str) -> Result<serde_json::Value> {
        Ok(self
            .bus(&[
                "get-property",
                "org.freedesktop.systemd1",
                path,
                "org.freedesktop.systemd1.Service",
                name,
            ])
            .await?["data"]
            .clone())
    }
    async fn dump(&self) -> Result<HashMap<String, Identity>> {
        let path = self.path().await?;
        let value = self
            .bus(&[
                "call",
                "org.freedesktop.systemd1",
                &path,
                "org.freedesktop.systemd1.Service",
                "DumpFileDescriptorStore",
            ])
            .await?;
        parse_dump(&value)
    }
    async fn require_owner(&self, path: &str) -> Result<()> {
        if self.property(path, "MainPID").await?.as_u64() != Some(std::process::id() as u64) {
            return Err("handover unit MainPID does not own this process".into());
        }
        Ok(())
    }
    pub(crate) async fn preflight(&self, needed: usize) -> Result<()> {
        let path = self.path().await?;
        self.require_owner(&path).await?;
        let restart = self.property(&path, "Restart").await?;
        if !matches!(restart.as_str(), Some("always" | "on-failure")) {
            return Err("handover requires Restart=always or on-failure (commit exits 75)".into());
        }
        let capacity = self
            .property(&path, "FileDescriptorStoreMax")
            .await?
            .as_u64()
            .ok_or("invalid fdstore capacity")?;
        let stored = self.dump().await?;
        if !stored.contains_key("listener") {
            return Err("handover requires a retained self-bound FDNAME=listener; socket-unit-only handover is not enabled".into());
        }
        if stored.keys().any(|n| n.starts_with("bp-")) {
            return Err("transient descriptors from an earlier generation remain; refusing to mix handovers".into());
        }
        if capacity < (stored.len() + needed) as u64 {
            return Err(
                "FileDescriptorStoreMax is too small for active SSE clients, source and manifest"
                    .into(),
            );
        }
        Ok(())
    }

    pub(crate) async fn invalidate(&self, socket: Option<&OsStr>, names: &[String]) -> Result<()> {
        // A wrong unit can appear empty while the actual unit retains a stale
        // restorable image. Verify custody before allowing any resumed I/O.
        self.require_owner(&self.path().await?).await?;
        for name in names {
            systemd::remove(socket, name)?;
        }
        systemd::barrier(socket)?;
        let stored = self.dump().await?;
        if names.iter().any(|n| stored.contains_key(n)) {
            return Err(
                "manager did not invalidate consumed handover state; refusing resumed I/O".into(),
            );
        }
        Ok(())
    }

    pub(crate) async fn reset(
        &self,
        control: &Controller,
        socket: Option<&OsStr>,
        successor: &Path,
        needed: usize,
    ) -> Result<()> {
        if socket.is_none_or(|s| s.is_empty()) {
            return Err("cooperative reset requires NOTIFY_SOCKET".into());
        }
        self.preflight(needed).await?;
        // The newly installed executable must advertise the exact format before
        // the old generation parks. It does not load credentials or touch sockets.
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::process::Command::new(successor)
                .args([
                    "--check-handover-format",
                    &handover::FORMAT_VERSION.to_string(),
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !result.status.success() {
            return Err("successor does not accept this handover format".into());
        }
        let frozen = tokio::time::timeout(Duration::from_secs(10), control.freeze())
            .await?
            .map_err(io::Error::other)?;
        let mut offered = Vec::new();
        let prepared = async {
            let bytes = handover::encode(frozen.image())?;
            let manifest = sealed_image(&bytes)?;
            let manifest_name = format!("bp-{}-manifest", frozen.image().generation);
            let mut descriptors = frozen.descriptors()?;
            let listener = descriptors.remove(0);
            let before = self.dump().await?;
            if before.get("listener") != Some(&identity(&listener.1)?) { return Err("stored listener does not match the parked listener".into()); }
            let path = self.path().await?;
            let capacity = self.property(&path, "FileDescriptorStoreMax").await?.as_u64().ok_or("invalid fdstore capacity")?;
            if capacity < (before.len() + descriptors.len() + 1) as u64 { return Err("fdstore capacity changed during preparation".into()); }
            descriptors.push((manifest_name, manifest)); // Manifest published last.
            for (name, fd) in &descriptors {
                offered.push(name.clone());
                systemd::store_transient(socket, name, fd.as_fd())?;
            }
            systemd::barrier(socket)?;
            let stored = self.dump().await?;
            for (name, fd) in &descriptors {
                if stored.get(name) != Some(&identity(fd)?) { return Err("manager rejected or misidentified a handover descriptor".into()); }
            }
            tracing::info!(generation = %frozen.image().generation, clients = frozen.image().clients.len(), "handover accepted by manager; relinquishing old process");
            // No destructors, socket shutdown, terminal chunks, epoch retirement
            // or telemetry flush after the cut. Systemd starts exactly one new owner.
            std::process::exit(75);
            #[allow(unreachable_code)] Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }.await;
        if let Err(error) = prepared {
            // Never resume a generation while a restorable stale image remains.
            if !offered.is_empty()
                && let Err(cleanup) = self.invalidate(socket, &offered).await
            {
                tracing::error!(%cleanup, "handover cleanup unverified; transports remain parked (operator must clear fdstore, then restart)");
                // Remain alive, with the cut immutable. A bounded reset failure
                // does not turn into an unplanned stale-snapshot restart.
                std::mem::forget(frozen);
                return Err(
                    "handover cleanup unverified; generation is parked, not resumed".into(),
                );
            }
            control.resume(frozen).await.map_err(io::Error::other)?;
            return Err(error);
        }
        Ok(())
    }
}

fn parse_dump(value: &serde_json::Value) -> Result<HashMap<String, Identity>> {
    let entries = value["data"][0]
        .as_array()
        .ok_or("invalid descriptor-store response")?;
    let mut output = HashMap::new();
    for entry in entries {
        let name = entry[0]
            .as_str()
            .ok_or("invalid descriptor name")?
            .to_owned();
        let number = |index| -> Result<u64> {
            entry[index]
                .as_u64()
                .ok_or_else(|| "invalid descriptor identity".into())
        };
        let id = Identity {
            mode: u32::try_from(number(1)?)?,
            major: u32::try_from(number(2)?)?,
            minor: u32::try_from(number(3)?)?,
            inode: number(4)?,
        };
        if output.insert(name, id).is_some() {
            return Err("duplicate descriptor-store names".into());
        }
    }
    Ok(output)
}

const SEALS: libc::c_int =
    libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
pub(crate) fn sealed_image(bytes: &[u8]) -> io::Result<OwnedFd> {
    let raw = unsafe {
        libc::memfd_create(
            c"obp-handover".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let mut file = std::fs::File::from(fd);
    file.write_all(bytes)?;
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, SEALS) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(file.into())
}
pub(crate) fn read_image(fd: OwnedFd) -> io::Result<handover::Image> {
    let seals = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GET_SEALS) };
    if seals < 0 || seals & SEALS != SEALS {
        return Err(io::Error::other("handover manifest must be sealed"));
    }
    let file = std::fs::File::from(fd);
    let length = file.metadata()?.len();
    if length > (handover::IMAGE_LIMIT + 48) as u64 {
        return Err(io::Error::other("handover manifest exceeds limit"));
    }
    let mut bytes = vec![0; length as usize];
    file.read_exact_at(&mut bytes, 0)?; // Open file descriptions share offsets.
    handover::decode(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sealed_memfd_rejects_writes_and_shared_offsets_are_irrelevant() {
        let fd = sealed_image(b"private bytes").unwrap();
        let copy = fd.as_fd().try_clone_to_owned().unwrap();
        let mut file = std::fs::File::from(copy);
        assert!(file.write_all(b"change").is_err());
        let mut bytes = [0; 13];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(&bytes, b"private bytes");
        assert!(
            read_image(std::fs::File::open("/dev/null").unwrap().into())
                .unwrap_err()
                .to_string()
                .contains("sealed")
        );
    }
    #[test]
    fn dump_checks_names_and_exact_kernel_identity_not_just_count() {
        let value = serde_json::json!({"type":"a(suuutuusu)","data":[[["bp-a-source",49152,0,9,123,0,0,"socket",2],["bp-a-manifest",32768,0,1,456,0,0,"memfd",2]]]});
        let dump = parse_dump(&value).unwrap();
        assert_eq!(dump["bp-a-source"].inode, 123);
        let duplicate = serde_json::json!({"data":[[["same",0,0,0,1],["same",0,0,0,2]]]});
        assert!(parse_dump(&duplicate).is_err());
    }
}
