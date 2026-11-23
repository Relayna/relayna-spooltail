//! Disk admission and volume accounting.
use crate::{Broker, Problem, Store, journal::partition_dir};
use anyhow::{Context, Result};
use serde::Serialize;
use std::{fs, path::Path};

const METADATA_ALLOWANCE: u64 = 64 * 1024;

#[derive(Default)]
pub(crate) struct Space {
    pub reserve: u64,
    pack_bytes: u64,
    packing_space_pauses: u64,
}

impl Space {
    pub fn configure() -> Result<Self> {
        let reserve = match std::env::var("SPOOLTAIL_DISK_RESERVE_BYTES") {
            Ok(value) => value
                .parse()
                .context("SPOOLTAIL_DISK_RESERVE_BYTES must be a nonnegative integer")?,
            Err(std::env::VarError::NotPresent) => 0,
            Err(error) => return Err(error).context("read SPOOLTAIL_DISK_RESERVE_BYTES"),
        };
        Ok(Self {
            reserve,
            ..Self::default()
        })
    }
}

#[cfg(unix)]
fn available(path: &Path) -> Result<u64> {
    use std::{ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt};
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stats = MaybeUninit::<libc::statvfs>::uninit();
    // statvfs initializes the object on success and does not retain either pointer.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("measure broker volume free space");
    }
    let stats = unsafe { stats.assume_init() };
    Ok((u128::from(stats.f_bavail) * u128::from(stats.f_frsize)).min(u128::from(u64::MAX)) as u64)
}

#[cfg(not(unix))]
fn available(_: &Path) -> Result<u64> {
    anyhow::bail!("volume space measurement requires Unix")
}

impl Store {
    fn append_reservations(&self) -> Result<u64> {
        Ok(self
            .db
            .query_row(
                "SELECT COALESCE(SUM(reserved_bytes),0) FROM appending",
                [],
                |r| r.get::<_, i64>(0),
            )?
            .try_into()?)
    }

    pub fn admit_disk_write(&self, body: u64, allow_reserve: bool) -> Result<u64> {
        if self.space.reserve == 0 {
            return Ok(0);
        }
        let needed = body.saturating_add(METADATA_ALLOWANCE);
        let floor = if allow_reserve { 0 } else { self.space.reserve };
        let threshold = floor
            .saturating_add(self.space.pack_bytes)
            .saturating_add(self.append_reservations()?)
            .saturating_add(needed);
        if available(self.data_dir())? < threshold {
            return Err(Problem::Capacity.into());
        }
        Ok(needed)
    }

    pub fn reserve_pack_space(&mut self, bytes: u64) -> Result<bool> {
        match self.admit_disk_write(bytes, false) {
            Ok(reserved) => {
                self.space.pack_bytes = reserved;
                Ok(true)
            }
            Err(error) if matches!(error.downcast_ref(), Some(Problem::Capacity)) => {
                self.space.packing_space_pauses += 1;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub fn release_pack_space(&mut self) {
        self.space.pack_bytes = 0;
    }
}

#[derive(Serialize)]
pub(crate) struct Stats {
    pub payload_reads: u64,
    pub recovered_frames: u64,
    pub automatic_task_moves: u64,
    pub free_bytes: u64,
    pub disk_reserve_bytes: u64,
    pub raw_journal_bytes: u64,
    pub packed_journal_bytes: u64,
    pub archive_pending_records: u64,
    pub packing_space_pauses: u64,
}

impl Broker {
    pub async fn stats(&self) -> Result<Stats> {
        let (mut stats, dir, partitions) = self
            .run(|store| {
                let pending: i64 = store.db.query_row(
                    "SELECT COALESCE(SUM((SELECT COUNT(*) FROM records AS r
                    WHERE r.topic=p.topic AND r.offset>=COALESCE(
                        (SELECT first_offset FROM floors WHERE topic=p.topic),1))),0)
                 FROM retention_policies AS p",
                    [],
                    |r| r.get(0),
                )?;
                Ok((
                    Stats {
                        payload_reads: store.payload_reads.get(),
                        recovered_frames: store.recovered_frames.try_into()?,
                        automatic_task_moves: store.automatic_moves,
                        free_bytes: 0,
                        disk_reserve_bytes: store.space.reserve,
                        raw_journal_bytes: 0,
                        packed_journal_bytes: 0,
                        archive_pending_records: pending.try_into()?,
                        packing_space_pauses: store.space.packing_space_pauses,
                    },
                    store.data_dir().to_owned(),
                    store.partition_ids(),
                ))
            })
            .await?;
        tokio::task::spawn_blocking(move || {
            stats.free_bytes = available(&dir)?;
            for partition in partitions {
                for entry in fs::read_dir(partition_dir(&dir, partition))? {
                    let entry = entry?;
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    let total = if name.ends_with(".seg") {
                        &mut stats.raw_journal_bytes
                    } else if name.ends_with(".seg.zst") {
                        &mut stats.packed_journal_bytes
                    } else {
                        continue;
                    };
                    match entry.metadata() {
                        Ok(m) if m.is_file() => *total += m.len(),
                        Ok(_) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => return Err(err.into()),
                    }
                }
            }
            Ok(stats)
        })
        .await?
    }
}
