//! Bounded, fixed single-member Deflate ZIP pages. No extraction/filesystem/network.
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};

use anyhow::{ensure, Result};
use zip::{write::FileOptions, CompressionMethod, ZipArchive, ZipWriter};

const MEMBER: &str = "response.json";

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    archive: usize,
    raw: usize,
}

impl Limits {
    pub fn new(archive: usize, raw: usize) -> Result<Self> {
        ensure!(archive > 0 && raw > 0, "limits must be positive");
        // Keep even worst-case Deflate expansion inside ZIP32 during finalization.
        ensure!(raw <= (u32::MAX / 2) as usize, "ZIP32 page limit exceeded");
        Ok(Self { archive, raw })
    }
}

struct Bounded<'a> {
    cursor: Cursor<&'a mut [u8]>,
    overflow: bool,
}

impl Write for Bounded<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let end = self.cursor.position().checked_add(buf.len() as u64);
        if !self.overflow && end.is_some_and(|n| n <= self.cursor.get_ref().len() as u64) {
            self.cursor.write_all(buf)?;
        } else {
            self.overflow = true;
            self.cursor.set_position(end.unwrap_or(u64::MAX));
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for Bounded<'_> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        Ok(self.cursor.seek(position).unwrap_or_else(|_| {
            self.overflow = true;
            self.cursor.position()
        }))
    }
}

#[cfg(test)]
thread_local! { static FINISHES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// Overflow is latched while ZIP finalization completes; partial bytes never escape.
pub fn encode(raw: &[u8], limits: Limits) -> Result<Vec<u8>> {
    ensure!(raw.len() <= limits.raw, "raw page exceeds limit");
    let mut storage = Vec::new();
    storage.try_reserve_exact(limits.archive)?;
    storage.resize(limits.archive, 0);
    let (used, overflow) = {
        let sink = Bounded {
            cursor: Cursor::new(storage.as_mut_slice()),
            overflow: false,
        };
        let mut writer = ZipWriter::new(sink);
        writer.start_file(
            MEMBER,
            FileOptions::default()
                .compression_method(CompressionMethod::Deflated)
                .last_modified_time(zip::DateTime::default())
                .unix_permissions(0o600),
        )?;
        writer.write_all(raw)?;
        let sink = writer.finish()?;
        #[cfg(test)]
        FINISHES.with(|n| n.set(n.get() + 1));
        (sink.cursor.position() as usize, sink.overflow)
    };
    ensure!(!overflow, "archive exceeds limit");
    storage.truncate(used);
    Ok(storage)
}

/// Match zip 0.6.6's last-signature selection, then validate before allocation.
fn preflight(bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() >= 22, "missing EOCD");
    let start = bytes.len().saturating_sub(22 + 65535);
    let at = (start..=bytes.len() - 22)
        .rev()
        .find(|&i| &bytes[i..i + 4] == b"PK\x05\x06")
        .ok_or_else(|| anyhow::anyhow!("missing EOCD"))?;
    let u16_at = |i| u16::from_le_bytes(bytes[at + i..at + i + 2].try_into().unwrap());
    let u32_at = |i| u32::from_le_bytes(bytes[at + i..at + i + 4].try_into().unwrap());
    ensure!(
        at + 22 + usize::from(u16_at(20)) == bytes.len(),
        "invalid EOCD comment"
    );
    ensure!(
        u16_at(4) == 0 && u16_at(6) == 0 && u16_at(8) == 1 && u16_at(10) == 1,
        "expected single disk/member"
    );
    let (size, offset) = (u32_at(12), u32_at(16));
    ensure!(
        size != u32::MAX
            && offset != u32::MAX
            && size >= 46
            && u64::from(offset) + u64::from(size) == at as u64,
        "invalid central directory bounds"
    );
    ensure!(
        at < 20 || &bytes[at - 20..at - 16] != b"PK\x06\x07",
        "ZIP64 not supported"
    );
    ensure!(
        &bytes[offset as usize..offset as usize + 4] == b"PK\x01\x02",
        "invalid central directory"
    );
    Ok(())
}

/// Read through EOF to force CRC validation; cap+1 detects lying size metadata.
pub fn decode(archive: &[u8], limits: Limits) -> Result<Vec<u8>> {
    ensure!(archive.len() <= limits.archive, "archive exceeds limit");
    preflight(archive)?;
    let mut zip = ZipArchive::new(Cursor::new(archive))?;
    ensure!(zip.len() == 1, "expected one ZIP member");
    let mut file = zip.by_index(0)?;
    ensure!(
        file.name() == MEMBER && !file.is_dir(),
        "unexpected ZIP member"
    );
    ensure!(
        file.compression() == CompressionMethod::Deflated,
        "expected Deflate"
    );
    ensure!(
        file.unix_mode()
            .is_none_or(|mode| matches!(mode & 0o170000, 0 | 0o100000)),
        "not a regular ZIP member"
    );
    let expected = file.size();
    ensure!(expected <= limits.raw as u64, "raw page exceeds limit");
    let mut raw = Vec::new();
    file.by_ref()
        .take(limits.raw as u64 + 1)
        .read_to_end(&mut raw)?;
    ensure!(
        raw.len() <= limits.raw && raw.len() as u64 == expected,
        "raw length mismatch"
    );
    Ok(raw)
}

#[cfg(test)]
mod tests;
