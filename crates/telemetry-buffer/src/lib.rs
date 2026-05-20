use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const SEGMENT_SUFFIX: &str = ".segment";
const CORRUPT_SUFFIX: &str = ".corrupt";
const CURSOR_FILE: &str = "cursor";
pub const RECORD_HEADER_LEN: u64 = 8;

#[derive(Debug)]
pub enum DiskQueueError {
    Io(std::io::Error),
    CorruptSegment(String),
    PayloadTooLarge(usize),
    Sink(String),
}

impl Display for DiskQueueError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "disk queue I/O error: {err}"),
            Self::CorruptSegment(message) => write!(f, "corrupt disk queue segment: {message}"),
            Self::PayloadTooLarge(size) => write!(f, "payload too large for disk queue: {size}"),
            Self::Sink(message) => write!(f, "disk queue sink failed: {message}"),
        }
    }
}

impl Error for DiskQueueError {}

impl From<std::io::Error> for DiskQueueError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskQueueOptions {
    pub max_segment_bytes: u64,
    pub fsync_on_write: bool,
}

impl Default for DiskQueueOptions {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024,
            fsync_on_write: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cursor {
    segment_id: u64,
    offset: u64,
}

enum ReadStep {
    Record(Vec<u8>, Cursor),
    Move(Cursor),
    Empty,
}

pub struct PendingBatch {
    payloads: Vec<Vec<u8>>,
    next_cursor: Cursor,
    should_commit: bool,
}

impl PendingBatch {
    pub fn payloads(&self) -> &[Vec<u8>] {
        &self.payloads
    }

    pub fn len(&self) -> usize {
        self.payloads.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }
}

pub struct DiskQueue {
    dir: PathBuf,
    options: DiskQueueOptions,
    active_segment_id: u64,
    active_segment_bytes: u64,
    cursor: Cursor,
}

impl DiskQueue {
    pub fn open(path: impl AsRef<Path>, options: DiskQueueOptions) -> Result<Self, DiskQueueError> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let segments = segment_ids(&dir)?;
        let active_segment_id = segments.iter().copied().max().unwrap_or(1);
        let active_path = segment_path(&dir, active_segment_id);
        if !active_path.exists() {
            File::create(&active_path)?;
        }
        let active_segment_bytes = fs::metadata(&active_path)?.len();

        let cursor = read_cursor(&dir)?.unwrap_or_else(|| Cursor {
            segment_id: segments.iter().copied().min().unwrap_or(active_segment_id),
            offset: 0,
        });
        persist_cursor(&dir, cursor)?;

        Ok(Self {
            dir,
            options,
            active_segment_id,
            active_segment_bytes,
            cursor,
        })
    }

    pub fn enqueue(&mut self, payload: &[u8]) -> Result<(), DiskQueueError> {
        if payload.len() > u32::MAX as usize {
            return Err(DiskQueueError::PayloadTooLarge(payload.len()));
        }

        let record_len = storage_bytes_for_payload(payload.len())?;
        if self.active_segment_bytes > 0
            && self.active_segment_bytes + record_len > self.options.max_segment_bytes
        {
            self.rotate_segment()?;
        }

        let path = segment_path(&self.dir, self.active_segment_id);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&checksum32(payload).to_le_bytes())?;
        file.write_all(payload)?;
        if self.options.fsync_on_write {
            file.sync_data()?;
        }
        self.active_segment_bytes += record_len;
        Ok(())
    }

    pub fn drain<F>(&mut self, max_items: usize, mut sink: F) -> Result<usize, DiskQueueError>
    where
        F: FnMut(&[u8]) -> Result<(), DiskQueueError>,
    {
        let mut drained = 0;
        while drained < max_items {
            match self.read_step_at(self.cursor)? {
                ReadStep::Record(payload, next_cursor) => {
                    sink(&payload)?;
                    self.commit_cursor(next_cursor)?;
                    drained += 1;
                }
                ReadStep::Move(next_cursor) => {
                    self.commit_cursor(next_cursor)?;
                }
                ReadStep::Empty => break,
            }
        }
        Ok(drained)
    }

    pub fn drain_batch<F>(&mut self, max_items: usize, mut sink: F) -> Result<usize, DiskQueueError>
    where
        F: FnMut(&[Vec<u8>]) -> Result<(), DiskQueueError>,
    {
        let batch = self.read_batch(max_items)?;
        if batch.is_empty() {
            self.commit_batch(batch)?;
            return Ok(0);
        }

        sink(batch.payloads())?;
        self.commit_batch(batch)
    }

    pub fn read_batch(&mut self, max_items: usize) -> Result<PendingBatch, DiskQueueError> {
        if max_items == 0 {
            return Ok(PendingBatch {
                payloads: Vec::new(),
                next_cursor: self.cursor,
                should_commit: false,
            });
        }

        let mut read_cursor = self.cursor;
        let mut final_cursor = self.cursor;
        let mut should_commit = false;
        let mut batch = Vec::with_capacity(max_items);

        while batch.len() < max_items {
            match self.read_step_at(read_cursor)? {
                ReadStep::Record(payload, next_cursor) => {
                    batch.push(payload);
                    read_cursor = next_cursor;
                    final_cursor = next_cursor;
                }
                ReadStep::Move(next_cursor) => {
                    read_cursor = next_cursor;
                    final_cursor = next_cursor;
                    should_commit = true;
                }
                ReadStep::Empty => break,
            }
        }

        Ok(PendingBatch {
            payloads: batch,
            next_cursor: final_cursor,
            should_commit,
        })
    }

    pub fn commit_batch(&mut self, batch: PendingBatch) -> Result<usize, DiskQueueError> {
        let len = batch.payloads.len();
        if len > 0 || batch.should_commit {
            self.commit_cursor(batch.next_cursor)?;
        }
        Ok(len)
    }

    pub fn queued_bytes(&self) -> Result<u64, DiskQueueError> {
        let mut total = 0;
        for id in segment_ids(&self.dir)? {
            total += fs::metadata(segment_path(&self.dir, id))?.len();
        }
        Ok(total)
    }

    pub fn cursor_position(&self) -> (u64, u64) {
        (self.cursor.segment_id, self.cursor.offset)
    }

    fn rotate_segment(&mut self) -> Result<(), DiskQueueError> {
        self.active_segment_id = self
            .active_segment_id
            .checked_add(1)
            .ok_or_else(|| DiskQueueError::CorruptSegment("segment id overflow".to_string()))?;
        self.active_segment_bytes = 0;
        File::create(segment_path(&self.dir, self.active_segment_id))?;
        Ok(())
    }

    fn read_step_at(&mut self, cursor: Cursor) -> Result<ReadStep, DiskQueueError> {
        let path = segment_path(&self.dir, cursor.segment_id);
        if !path.exists() {
            return self
                .next_cursor_after(cursor.segment_id)
                .map(|cursor| cursor.map_or(ReadStep::Empty, ReadStep::Move));
        }

        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(cursor.offset))?;

        let mut header = [0_u8; RECORD_HEADER_LEN as usize];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                if cursor.segment_id < self.active_segment_id {
                    return self
                        .next_cursor_after(cursor.segment_id)
                        .map(|cursor| cursor.map_or(ReadStep::Empty, ReadStep::Move));
                }
                return Ok(ReadStep::Empty);
            }
            Err(err) => return Err(DiskQueueError::Io(err)),
        }

        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let expected_checksum = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let mut payload = vec![0_u8; len];
        if let Err(err) = file.read_exact(&mut payload) {
            if err.kind() == ErrorKind::UnexpectedEof {
                return self.quarantine_and_advance(
                    cursor.segment_id,
                    format!(
                        "truncated record in segment {} at offset {}",
                        cursor.segment_id, cursor.offset
                    ),
                );
            }
            return Err(DiskQueueError::Io(err));
        }

        let actual_checksum = checksum32(&payload);
        if actual_checksum != expected_checksum {
            return self.quarantine_and_advance(
                cursor.segment_id,
                format!(
                    "checksum mismatch in segment {} at offset {}",
                    cursor.segment_id, cursor.offset
                ),
            );
        }

        Ok(ReadStep::Record(
            payload,
            Cursor {
                segment_id: cursor.segment_id,
                offset: cursor.offset + RECORD_HEADER_LEN + len as u64,
            },
        ))
    }

    fn quarantine_and_advance(
        &mut self,
        segment_id: u64,
        reason: String,
    ) -> Result<ReadStep, DiskQueueError> {
        self.quarantine_segment(segment_id, &reason)?;

        if segment_id >= self.active_segment_id {
            self.active_segment_id = segment_id
                .checked_add(1)
                .ok_or_else(|| DiskQueueError::CorruptSegment("segment id overflow".to_string()))?;
            self.active_segment_bytes = 0;
            File::create(segment_path(&self.dir, self.active_segment_id))?;
            return Ok(ReadStep::Move(Cursor {
                segment_id: self.active_segment_id,
                offset: 0,
            }));
        }

        Ok(self
            .next_cursor_after(segment_id)?
            .map_or(ReadStep::Empty, ReadStep::Move))
    }

    fn quarantine_segment(&self, segment_id: u64, reason: &str) -> Result<(), DiskQueueError> {
        let source = segment_path(&self.dir, segment_id);
        if !source.exists() {
            return Ok(());
        }

        let destination = corrupt_segment_path(&source)?;
        fs::rename(&source, &destination)?;
        fs::write(quarantine_reason_path(&destination), format!("{reason}\n"))?;
        Ok(())
    }

    fn commit_cursor(&mut self, cursor: Cursor) -> Result<(), DiskQueueError> {
        self.cursor = cursor;
        self.remove_segments_before(cursor.segment_id)?;
        persist_cursor(&self.dir, self.cursor)?;
        Ok(())
    }

    fn remove_segments_before(&self, segment_id: u64) -> Result<(), DiskQueueError> {
        for id in segment_ids(&self.dir)?
            .into_iter()
            .filter(|id| *id < segment_id)
        {
            let path = segment_path(&self.dir, id);
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }

    fn next_cursor_after(&self, current: u64) -> Result<Option<Cursor>, DiskQueueError> {
        Ok(self.next_segment_after(current)?.map(|segment_id| Cursor {
            segment_id,
            offset: 0,
        }))
    }

    fn next_segment_after(&self, current: u64) -> Result<Option<u64>, DiskQueueError> {
        Ok(segment_ids(&self.dir)?
            .into_iter()
            .filter(|id| *id > current)
            .min())
    }
}

fn segment_ids(dir: &Path) -> Result<Vec<u64>, DiskQueueError> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some(id) = name.strip_suffix(SEGMENT_SUFFIX) else {
            continue;
        };
        if let Ok(id) = id.parse::<u64>() {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}{SEGMENT_SUFFIX}"))
}

fn corrupt_segment_path(source: &Path) -> Result<PathBuf, DiskQueueError> {
    for suffix in 0..1000 {
        let candidate = if suffix == 0 {
            PathBuf::from(format!("{}{}", source.display(), CORRUPT_SUFFIX))
        } else {
            PathBuf::from(format!("{}{}.{suffix}", source.display(), CORRUPT_SUFFIX))
        };

        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(DiskQueueError::CorruptSegment(format!(
        "could not allocate quarantine path for {}",
        source.display()
    )))
}

fn quarantine_reason_path(corrupt_segment: &Path) -> PathBuf {
    PathBuf::from(format!("{}.reason", corrupt_segment.display()))
}

pub fn storage_bytes_for_payload(payload_len: usize) -> Result<u64, DiskQueueError> {
    if payload_len > u32::MAX as usize {
        return Err(DiskQueueError::PayloadTooLarge(payload_len));
    }

    Ok(RECORD_HEADER_LEN + payload_len as u64)
}

fn read_cursor(dir: &Path) -> Result<Option<Cursor>, DiskQueueError> {
    let path = dir.join(CURSOR_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path)?;
    let mut parts = content.split_whitespace();
    let Some(segment_id) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
        return Ok(None);
    };
    let Some(offset) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
        return Ok(None);
    };
    Ok(Some(Cursor { segment_id, offset }))
}

fn persist_cursor(dir: &Path, cursor: Cursor) -> Result<(), DiskQueueError> {
    let tmp = dir.join("cursor.tmp");
    let final_path = dir.join(CURSOR_FILE);
    fs::write(&tmp, format!("{} {}\n", cursor.segment_id, cursor.offset))?;
    fs::rename(tmp, final_path)?;
    Ok(())
}

fn checksum32(payload: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5_u32;
    for byte in payload {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn enqueue_and_drain_preserves_order() -> Result<(), DiskQueueError> {
        let dir = test_dir("order");
        let mut queue = DiskQueue::open(
            &dir,
            DiskQueueOptions {
                max_segment_bytes: 16,
                fsync_on_write: false,
            },
        )?;

        queue.enqueue(b"one")?;
        queue.enqueue(b"two")?;

        let mut drained = Vec::new();
        let count = queue.drain(10, |payload| {
            drained.push(payload.to_vec());
            Ok(())
        })?;

        assert_eq!(count, 2);
        assert_eq!(drained, vec![b"one".to_vec(), b"two".to_vec()]);
        cleanup(dir);
        Ok(())
    }

    #[test]
    fn failed_sink_keeps_record_for_retry() -> Result<(), DiskQueueError> {
        let dir = test_dir("retry");
        let mut queue = DiskQueue::open(&dir, DiskQueueOptions::default())?;

        queue.enqueue(b"retry-me")?;

        let result = queue.drain(1, |_payload| Err(DiskQueueError::Sink("stop".to_string())));
        assert!(result.is_err());

        let mut drained = Vec::new();
        queue.drain(1, |payload| {
            drained.push(payload.to_vec());
            Ok(())
        })?;

        assert_eq!(drained, vec![b"retry-me".to_vec()]);
        cleanup(dir);
        Ok(())
    }

    #[test]
    fn failed_batch_sink_keeps_entire_batch_for_retry() -> Result<(), DiskQueueError> {
        let dir = test_dir("batch-retry");
        let mut queue = DiskQueue::open(&dir, DiskQueueOptions::default())?;

        queue.enqueue(b"one")?;
        queue.enqueue(b"two")?;

        let result = queue.drain_batch(2, |_payloads| {
            Err(DiskQueueError::Sink("export failed".to_string()))
        });
        assert!(result.is_err());

        let mut drained = Vec::new();
        queue.drain_batch(2, |payloads| {
            drained.extend(payloads.iter().cloned());
            Ok(())
        })?;

        assert_eq!(drained, vec![b"one".to_vec(), b"two".to_vec()]);
        cleanup(dir);
        Ok(())
    }

    #[test]
    fn corrupt_segment_is_quarantined_and_later_segment_is_read() -> Result<(), DiskQueueError> {
        let dir = test_dir("corrupt-segment");
        let mut queue = DiskQueue::open(
            &dir,
            DiskQueueOptions {
                max_segment_bytes: 16,
                fsync_on_write: false,
            },
        )?;

        queue.enqueue(b"one")?;
        queue.enqueue(b"two")?;

        let first_segment = segment_path(&dir, 1);
        let mut file = OpenOptions::new().write(true).open(&first_segment)?;
        file.seek(SeekFrom::Start(RECORD_HEADER_LEN))?;
        file.write_all(b"x")?;
        drop(file);

        let mut drained = Vec::new();
        let count = queue.drain(10, |payload| {
            drained.push(payload.to_vec());
            Ok(())
        })?;

        assert_eq!(count, 1);
        assert_eq!(drained, vec![b"two".to_vec()]);
        assert!(!first_segment.exists());
        assert!(PathBuf::from(format!("{}{}", first_segment.display(), CORRUPT_SUFFIX)).exists());
        cleanup(dir);
        Ok(())
    }

    fn test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("telemetry_buffer_{name}_{nanos}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn cleanup(path: PathBuf) {
        let _ = fs::remove_dir_all(path);
    }
}
