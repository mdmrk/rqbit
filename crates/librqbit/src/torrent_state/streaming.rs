use std::{
    collections::VecDeque,
    io::SeekFrom,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::Instant,
};

use anyhow::Context;
use dashmap::DashMap;

use librqbit_core::lengths::{CurrentPiece, Lengths, ValidPieceIndex};
use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::OwnedSemaphorePermit,
};
use tracing::{debug, trace};

use crate::{ManagedTorrent, file_info::FileInfo, storage::TorrentStorage};

use super::{ManagedTorrentHandle, TorrentMetadata};

type StreamId = usize;

// 32 mb lookahead by default.
const PER_STREAM_BUF_DEFAULT: u64 = 32 * 1024 * 1024;

struct StreamState {
    file_id: usize,
    file_len: u64,
    file_abs_offset: u64,
    position: u64,
    waker: Option<Waker>,
}

impl StreamState {
    fn current_piece(&self, lengths: &Lengths) -> Option<CurrentPiece> {
        lengths.compute_current_piece(self.position, self.file_abs_offset)
    }

    fn span(&self, lookahead: u64) -> std::ops::Range<u64> {
        let start = self.file_abs_offset + self.position;
        start..(start + lookahead).min(self.file_abs_offset + self.file_len)
    }

    fn queue<'a>(
        &self,
        lengths: &'a Lengths,
        lookahead: u64,
    ) -> impl Iterator<Item = ValidPieceIndex> + use<'a> {
        let std::ops::Range { start, end } = self.span(lookahead);
        let dpl = lengths.default_piece_length();
        let start_id = (start / dpl as u64).try_into().unwrap();
        let end_id = if lookahead == 0 {
            start_id
        } else {
            end.div_ceil(dpl as u64).try_into().unwrap()
        };
        (start_id..end_id).filter_map(|i| lengths.validate_piece_index(i))
    }
}

pub(crate) struct TorrentStreams {
    next_stream_id: AtomicUsize,
    streams: DashMap<StreamId, StreamState>,
    lookahead: AtomicU64,
    default_lookahead: u64,
    window: Option<u64>,
}

impl Default for TorrentStreams {
    fn default() -> Self {
        Self::new(None)
    }
}

pub(crate) fn pieces_to_forget(
    have: impl Iterator<Item = ValidPieceIndex>,
    lengths: &Lengths,
    spans: &[std::ops::Range<u64>],
    mut held: u64,
    budget: u64,
) -> Vec<ValidPieceIndex> {
    if held <= budget || spans.is_empty() {
        return Vec::new();
    }
    let mut candidates: Vec<(u64, ValidPieceIndex)> = have
        .filter_map(|piece| {
            let start = lengths.piece_offset(piece);
            let end = start + lengths.piece_length(piece) as u64;
            spans
                .iter()
                .map(|span| {
                    if end <= span.start {
                        Some(span.start - end)
                    } else if start >= span.end.max(span.start + 1) {
                        Some(start - span.end)
                    } else {
                        None
                    }
                })
                .try_fold(u64::MAX, |nearest, d| Some(nearest.min(d?)))
                .map(|distance| (distance, piece))
        })
        .collect();
    candidates.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(b.1.get().cmp(&a.1.get())));
    let mut forget = Vec::new();
    for (_, piece) in candidates {
        if held <= budget {
            break;
        }
        held -= lengths.piece_length(piece) as u64;
        forget.push(piece);
    }
    forget
}

impl TorrentStreams {
    pub(crate) fn new(window: Option<u64>) -> Self {
        let default_lookahead = window.map_or(PER_STREAM_BUF_DEFAULT, |w| w / 4 * 3);
        Self {
            next_stream_id: AtomicUsize::new(0),
            streams: DashMap::new(),
            lookahead: AtomicU64::new(default_lookahead),
            default_lookahead,
            window,
        }
    }

    fn lookahead(&self) -> u64 {
        self.lookahead.load(Ordering::Relaxed)
    }

    pub(crate) fn set_lookahead(&self, bytes: Option<u64>) -> bool {
        let value = bytes.map_or(self.default_lookahead, |b| b.min(self.default_lookahead));
        self.lookahead.swap(value, Ordering::Relaxed) != value
    }

    pub(crate) fn window(&self) -> Option<u64> {
        self.window
    }

    pub(crate) fn spans(&self) -> Vec<std::ops::Range<u64>> {
        self.streams
            .iter()
            .map(|s| s.value().span(self.lookahead()))
            .collect()
    }

    fn next_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    fn register_waker(&self, stream_id: StreamId, waker: Waker) {
        if let Some(mut s) = self.streams.get_mut(&stream_id) {
            let vm = s.value_mut();
            vm.waker = Some(waker);
        }
    }

    // Interleave 1st, 2nd etc pieces from each active stream in turn until they get 1/10th of the file .
    pub(crate) fn iter_next_pieces<'a>(
        &'a self,
        lengths: &'a Lengths,
    ) -> impl Iterator<Item = ValidPieceIndex> + 'a {
        struct Interleave<I> {
            all: VecDeque<I>,
        }

        impl<I: Iterator<Item = ValidPieceIndex>> Iterator for Interleave<I> {
            type Item = ValidPieceIndex;

            fn next(&mut self) -> Option<Self::Item> {
                while let Some(mut it) = self.all.pop_front() {
                    if let Some(piece) = it.next() {
                        self.all.push_back(it);
                        return Some(piece);
                    }
                }
                None
            }
        }

        let mut all: Vec<_> = self
            .streams
            .iter()
            .map(|s| s.queue(lengths, self.lookahead()))
            .collect();

        // Shuffle to decrease determinism and make queueing fairer.
        use rand::seq::SliceRandom;
        all.shuffle(&mut rand::rng());

        Interleave { all: all.into() }
    }

    pub(crate) fn wake_streams_on_piece_completed(
        &self,
        piece_id: ValidPieceIndex,
        lengths: &Lengths,
    ) {
        for mut w in self.streams.iter_mut() {
            if w.value().current_piece(lengths).map(|p| p.id) == Some(piece_id)
                && let Some(waker) = w.value_mut().waker.take()
            {
                debug!(
                    stream_id = *w.key(),
                    piece_id = piece_id.get(),
                    "waking stream"
                );
                waker.wake();
            }
        }
    }

    fn drop_stream(&self, stream_id: StreamId) -> Option<StreamState> {
        debug!(stream_id, "dropping stream");
        self.streams.remove(&stream_id).map(|s| s.1)
    }

    pub(crate) fn streamed_file_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.streams.iter().map(|s| s.value().file_id)
    }
}

pub struct FileStream {
    torrent: ManagedTorrentHandle,
    metadata: Arc<TorrentMetadata>,
    streams: Arc<TorrentStreams>,
    stream_id: usize,
    file_id: usize,
    position: u64,

    // file params
    file_len: u64,
    file_torrent_abs_offset: u64,

    _blocking_permit: OwnedSemaphorePermit,
}

macro_rules! map_io_err {
    ($e:expr) => {
        $e.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    };
}

macro_rules! poll_try_io {
    ($e:expr) => {{
        let e = map_io_err!($e);
        match e {
            Ok(r) => r,
            Err(e) => {
                debug!("stream error {e:#}");
                return Poll::Ready(Err(e));
            }
        }
    }};
}

impl AsyncRead for FileStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        tbuf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // if the file is over, return 0
        if self.position == self.file_len {
            debug!(
                stream_id = self.stream_id,
                file_id = self.file_id,
                "stream completed, EOF"
            );
            return Poll::Ready(Ok(()));
        }

        let current = poll_try_io!(
            self.metadata
                .lengths()
                .compute_current_piece(self.position, self.file_torrent_abs_offset)
                .context("invalid position")
        );

        // if the piece is not there, register to wake when it is
        // check if we have the piece for real
        let have = poll_try_io!(self.torrent.with_chunk_tracker(|ct| {
            let have = ct.get_have_pieces().as_slice()[current.id.get() as usize];
            if !have {
                self.streams
                    .register_waker(self.stream_id, cx.waker().clone());
            }
            have
        }));
        if !have {
            debug!(stream_id = self.stream_id, file_id = self.file_id, piece_id = %current.id, "poll pending, not have");
            return Poll::Pending;
        }

        // actually stream the piece
        let buf = tbuf.initialize_unfilled();
        let file_remaining = self.file_len - self.position;
        let bytes_to_read: usize = poll_try_io!(
            (buf.len() as u64)
                .min(current.piece_remaining as u64)
                .min(file_remaining)
                .try_into()
        );

        let buf = &mut buf[..bytes_to_read];

        let start = Instant::now();
        poll_try_io!(poll_try_io!(self.torrent.shared.spawner.block_in_place(
            || {
                self.torrent.with_storage_and_file(
                    self.file_id,
                    |files, _fi| {
                        files.pread_exact(self.file_id, self.position, buf)?;
                        Ok::<_, anyhow::Error>(())
                    },
                    &self.metadata,
                )
            }
        )));

        trace!(
            buflen = buf.len(),
            stream_id = self.stream_id,
            file_id = self.file_id,
            read_time = ?start.elapsed(),
            "will write bytes"
        );

        self.as_mut().advance(bytes_to_read as u64);
        tbuf.advance(bytes_to_read);

        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for FileStream {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let end_i64 = map_io_err!(TryInto::<i64>::try_into(self.file_len))?;
        let new_pos: i64 = match position {
            SeekFrom::Start(s) => map_io_err!(s.try_into())?,
            SeekFrom::End(e) => map_io_err!(TryInto::<i64>::try_into(self.file_len))? + e,
            SeekFrom::Current(o) => map_io_err!(TryInto::<i64>::try_into(self.position))? + o,
        };

        if new_pos < 0 || new_pos > end_i64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                anyhow::anyhow!("invalid seek"),
            ));
        }

        self.as_mut().set_position(map_io_err!(new_pos.try_into())?);
        debug!(stream_id = self.stream_id, position = self.position, "seek");
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

impl Drop for FileStream {
    fn drop(&mut self) {
        self.streams.drop_stream(self.stream_id);
    }
}

impl ManagedTorrent {
    fn with_storage_and_file<F, R>(
        &self,
        file_id: usize,
        f: F,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<R>
    where
        F: FnOnce(&dyn TorrentStorage, &FileInfo) -> R,
    {
        self.with_state(|s| {
            let files = match s {
                crate::ManagedTorrentState::Paused(p) => &*p.files,
                crate::ManagedTorrentState::Live(l) => &*l.files,
                s => anyhow::bail!("with_storage_and_file: invalid state: {}", s.name()),
            };
            let fi = metadata.file_infos.get(file_id).context("invalid file")?;
            Ok(f(files, fi))
        })
    }

    fn streams(&self) -> anyhow::Result<Arc<TorrentStreams>> {
        self.with_state(|s| match s {
            crate::ManagedTorrentState::Paused(p) => Ok(p.streams.clone()),
            crate::ManagedTorrentState::Live(l) => Ok(l.streams.clone()),
            s => anyhow::bail!("streams: invalid state {}", s.name()),
        })
    }

    fn maybe_reconnect_needed_peers_for_file(&self, file_id: usize) -> bool {
        // If we have the full file, don't bother.
        if self.is_file_finished(file_id) {
            return false;
        }
        self.with_state(|state| {
            if let crate::ManagedTorrentState::Live(l) = &state {
                l.reconnect_all_not_needed_peers();
            }
        });
        true
    }

    pub fn set_stream_lookahead(&self, bytes: Option<u64>) -> anyhow::Result<()> {
        if self.streams()?.set_lookahead(bytes) {
            self.wake_piece_requesters();
        }
        Ok(())
    }

    fn wake_piece_requesters(&self) {
        self.with_state(|s| {
            if let crate::ManagedTorrentState::Live(l) = s {
                l.wake_piece_requesters();
            }
        });
    }

    fn is_file_finished(&self, file_id: usize) -> bool {
        let metadata = self.metadata.load();
        let metadata = match metadata.as_ref() {
            Some(r) => r,
            None => return false,
        };
        // TODO: would be nice to remove locking
        self.with_chunk_tracker(|ct| ct.is_file_finished(&metadata.file_infos[file_id]))
            .unwrap_or(false)
    }

    pub async fn stream(self: Arc<Self>, file_id: usize) -> anyhow::Result<FileStream> {
        // Streams need live or paused state, so wait out the initial check
        // of existing data instead of failing while it runs.
        self.wait_until_initialized().await?;
        let metadata = self
            .metadata
            .load_full()
            .context("torrent metadata is not resolved")?;
        let (fd_len, fd_offset) = self.with_storage_and_file(
            file_id,
            |_fd, fi| (fi.len, fi.offset_in_torrent),
            &metadata,
        )?;
        let streams = self.streams()?;
        let blocking_permit = self.shared().spawner.semaphore().acquire_owned().await?;
        let s = FileStream {
            stream_id: streams.next_id(),
            streams: streams.clone(),
            file_id,
            position: 0,

            file_len: fd_len,
            file_torrent_abs_offset: fd_offset,
            _blocking_permit: blocking_permit,
            torrent: self,
            metadata,
        };
        s.torrent.maybe_reconnect_needed_peers_for_file(file_id);
        let windowed = streams.window.is_some();
        streams.streams.insert(
            s.stream_id,
            StreamState {
                file_id,
                position: 0,
                waker: None,
                file_len: fd_len,
                file_abs_offset: fd_offset,
            },
        );

        if windowed {
            s.torrent.wake_piece_requesters();
        }

        debug!(stream_id = s.stream_id, file_id, "started stream");

        Ok(s)
    }
}

impl FileStream {
    pub fn position(&self) -> u64 {
        self.position
    }

    fn advance(&mut self, diff: u64) {
        self.set_position(self.position + diff)
    }

    fn set_position(&mut self, new_pos: u64) {
        if self.streams.window.is_some() {
            let dpl = self.metadata.lengths().default_piece_length() as u64;
            let abs = self.file_torrent_abs_offset;
            if (abs + self.position) / dpl != (abs + new_pos) / dpl {
                self.torrent.wake_piece_requesters();
            }
        }
        self.position = new_pos;
        self.streams
            .streams
            .get_mut(&self.stream_id)
            .unwrap()
            .value_mut()
            .position = new_pos;
    }

    pub fn len(&self) -> u64 {
        self.file_len
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use librqbit_core::lengths::{Lengths, ValidPieceIndex};

    use super::{StreamState, TorrentStreams, pieces_to_forget};

    const PIECE: u32 = 16384;

    fn setup() -> (Lengths, Vec<ValidPieceIndex>) {
        let l = Lengths::new(PIECE as u64 * 10, PIECE).unwrap();
        let all = (0..10)
            .map(|i| l.validate_piece_index(i).unwrap())
            .collect();
        (l, all)
    }

    fn ids(pieces: Vec<ValidPieceIndex>) -> Vec<u32> {
        pieces.into_iter().map(|p| p.get()).collect()
    }

    #[test]
    fn the_lookahead_can_shrink_but_not_grow_past_the_default() {
        let streams = TorrentStreams::new(Some(1000));
        assert_eq!(streams.lookahead(), 750);
        assert!(streams.set_lookahead(Some(100)));
        assert!(!streams.set_lookahead(Some(100)));
        assert_eq!(streams.lookahead(), 100);
        streams.set_lookahead(Some(5000));
        assert_eq!(streams.lookahead(), 750);
        streams.set_lookahead(Some(0));
        assert_eq!(streams.lookahead(), 0);
        streams.set_lookahead(None);
        assert_eq!(streams.lookahead(), 750);
    }

    #[test]
    fn a_zero_lookahead_queues_nothing_not_even_the_current_piece() {
        let (l, _) = setup();
        let stream = StreamState {
            file_id: 0,
            file_len: PIECE as u64 * 10,
            file_abs_offset: 0,
            position: PIECE as u64 * 3 + 10,
            waker: None,
        };
        assert_eq!(stream.queue(&l, 0).count(), 0);
        let one: Vec<u32> = stream.queue(&l, 1).map(|p| p.get()).collect();
        assert_eq!(one, vec![3]);
    }

    #[test]
    fn nothing_is_forgotten_within_budget_or_without_streams() {
        let (l, all) = setup();
        let held = PIECE as u64 * 10;
        assert!(pieces_to_forget(all.iter().copied(), &l, &[0..1], held, held).is_empty());
        assert!(pieces_to_forget(all.iter().copied(), &l, &[], held, 0).is_empty());
    }

    #[test]
    fn the_farthest_pieces_go_first_and_spans_are_kept() {
        let (l, all) = setup();
        let p = PIECE as u64;
        let span = 4 * p + 10..7 * p;
        let forget = pieces_to_forget(
            all.iter().copied(),
            &l,
            std::slice::from_ref(&span),
            10 * p,
            6 * p,
        );
        assert_eq!(ids(forget), vec![0, 1, 9, 2]);
        let forget = pieces_to_forget(all.iter().copied(), &l, &[span], 10 * p, 0);
        assert_eq!(ids(forget), vec![0, 1, 9, 2, 8, 3, 7]);
    }

    #[test]
    fn a_piece_near_any_stream_is_near() {
        let (l, all) = setup();
        let p = PIECE as u64;
        let forget = pieces_to_forget(
            all.iter().copied(),
            &l,
            &[0..p, 9 * p..10 * p],
            10 * p,
            8 * p,
        );
        assert_eq!(ids(forget), vec![5, 4]);
    }

    #[test]
    fn a_stream_at_the_end_keeps_the_last_piece() {
        let (l, all) = setup();
        let p = PIECE as u64;
        let forget = pieces_to_forget(all.iter().copied(), &l, &[10 * p..10 * p], 10 * p, 0);
        assert_eq!(
            forget.len(),
            10,
            "an empty span at EOF protects nothing past it"
        );
        let forget = pieces_to_forget(all.iter().copied(), &l, &[9 * p + 1..9 * p + 1], 10 * p, 0);
        assert!(
            !ids(forget).contains(&9),
            "the piece the stream is in stays"
        );
    }
}
