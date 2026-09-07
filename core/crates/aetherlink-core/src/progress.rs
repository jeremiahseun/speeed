//! Progress reporting.
//!
//! The engine emits progress far faster than any UI can use it — at 1 GB/s with
//! 4 MB chunks that is 250 events a second, each one crossing the FFI boundary
//! and hopping to a main thread. [`Throttled`] collapses that to a fixed rate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Implemented by the platform layer. Every method must return promptly: these
/// are called from stream workers, and blocking here stalls the transfer.
pub trait ProgressSink: Send + Sync {
    fn on_progress(&self, bytes_done: u64, bytes_total: u64);
    fn on_file_completed(&self, file_id: u64, path: &str);
}

/// Discards everything. Used by the CLI and by tests.
pub struct NoProgress;

impl ProgressSink for NoProgress {
    fn on_progress(&self, _bytes_done: u64, _bytes_total: u64) {}
    fn on_file_completed(&self, _file_id: u64, _path: &str) {}
}

/// Rate-limits `on_progress` to at most one call per interval. Completion
/// events are never dropped — they are rare and each one matters.
///
/// The final call of a transfer is also always delivered, so a UI never sticks
/// at 99%.
pub struct Throttled {
    inner: Arc<dyn ProgressSink>,
    interval: Duration,
    origin: Instant,
    /// Micros since `origin` at the last emitted update, or [`Self::NEVER`]
    /// before the first one. A plain zero would not do: zero is a legitimate
    /// timestamp, and treating it as "already emitted" swallows the first
    /// update — leaving the UI blank for a whole interval, or for the entire
    /// transfer if it is shorter than one.
    last_emit_micros: AtomicU64,
}

impl Throttled {
    /// 100 ms — ten updates a second, which is smooth to the eye and cheap.
    pub const DEFAULT_INTERVAL: Duration = Duration::from_millis(100);

    /// Sentinel for "no update has been emitted yet".
    const NEVER: u64 = u64::MAX;

    pub fn new(inner: Arc<dyn ProgressSink>, interval: Duration) -> Self {
        Self {
            inner,
            interval,
            origin: Instant::now(),
            last_emit_micros: AtomicU64::new(Self::NEVER),
        }
    }

    pub fn with_default_interval(inner: Arc<dyn ProgressSink>) -> Self {
        Self::new(inner, Self::DEFAULT_INTERVAL)
    }
}

impl ProgressSink for Throttled {
    fn on_progress(&self, bytes_done: u64, bytes_total: u64) {
        // The last update must land regardless of timing, or the UI stalls
        // short of complete.
        let is_final = bytes_total > 0 && bytes_done >= bytes_total;
        let now = self.origin.elapsed().as_micros() as u64;
        let interval = self.interval.as_micros() as u64;

        if !is_final {
            let last = self.last_emit_micros.load(Ordering::Relaxed);
            // The first update always goes through; a caller who has just
            // started deserves to see that something is happening.
            if last != Self::NEVER && now.saturating_sub(last) < interval {
                return;
            }
            // Racing workers may both pass the check; whoever wins the swap
            // emits. A duplicate update would be harmless, but this keeps the
            // rate honest.
            if self
                .last_emit_micros
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
            {
                return;
            }
        }
        self.inner.on_progress(bytes_done, bytes_total);
    }

    fn on_file_completed(&self, file_id: u64, path: &str) {
        self.inner.on_file_completed(file_id, path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        updates: Mutex<Vec<(u64, u64)>>,
        completions: Mutex<Vec<u64>>,
    }

    impl ProgressSink for Recorder {
        fn on_progress(&self, done: u64, total: u64) {
            self.updates.lock().unwrap().push((done, total));
        }
        fn on_file_completed(&self, file_id: u64, _path: &str) {
            self.completions.lock().unwrap().push(file_id);
        }
    }

    #[test]
    fn the_first_update_is_never_withheld() {
        let rec = Arc::new(Recorder::default());
        let throttled = Throttled::new(rec.clone(), Duration::from_secs(3600));
        throttled.on_progress(1, 1_000_000);
        assert_eq!(
            rec.updates.lock().unwrap().as_slice(),
            &[(1, 1_000_000)],
            "a UI must not sit blank for a whole interval at the start"
        );
    }

    #[test]
    fn collapses_a_burst_into_a_single_update() {
        let rec = Arc::new(Recorder::default());
        let throttled = Throttled::new(rec.clone(), Duration::from_secs(3600));
        for i in 1..=1000 {
            throttled.on_progress(i, 10_000);
        }
        assert_eq!(
            rec.updates.lock().unwrap().len(),
            1,
            "a burst inside one interval must emit once"
        );
    }

    #[test]
    fn always_delivers_the_final_update() {
        let rec = Arc::new(Recorder::default());
        let throttled = Throttled::new(rec.clone(), Duration::from_secs(3600));
        throttled.on_progress(10, 100);
        throttled.on_progress(50, 100);
        throttled.on_progress(100, 100);

        let updates = rec.updates.lock().unwrap();
        assert_eq!(
            updates.last(),
            Some(&(100, 100)),
            "the completing update must never be throttled away"
        );
    }

    #[test]
    fn never_drops_a_completion_event() {
        let rec = Arc::new(Recorder::default());
        let throttled = Throttled::new(rec.clone(), Duration::from_secs(3600));
        for id in 0..50 {
            throttled.on_file_completed(id, "x");
        }
        assert_eq!(rec.completions.lock().unwrap().len(), 50);
    }

    #[test]
    fn emits_again_once_the_interval_passes() {
        let rec = Arc::new(Recorder::default());
        let throttled = Throttled::new(rec.clone(), Duration::from_millis(1));
        throttled.on_progress(1, 100);
        std::thread::sleep(Duration::from_millis(5));
        throttled.on_progress(2, 100);
        assert_eq!(rec.updates.lock().unwrap().len(), 2);
    }
}
