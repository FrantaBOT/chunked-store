use arc_swap::ArcSwapOption;
use async_stream::try_stream;
use axum::body::Bytes;
use futures_util::Stream;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
struct Chunk {
    data: Bytes,
    next: Arc<ArcSwapOption<Chunk>>,
}

pub struct Segment {
    start_chunk: Arc<ArcSwapOption<Chunk>>,
    current_chunk: Arc<ArcSwapOption<Chunk>>,
    notify: Arc<Notify>,
    closed: AtomicBool,
}

impl Segment {
    pub fn new() -> Self {
        Self {
            start_chunk: Arc::new(ArcSwapOption::new(None)),
            current_chunk: Arc::new(ArcSwapOption::new(None)),
            notify: Arc::new(Notify::new()),
            closed: AtomicBool::new(false),
        }
    }

    pub fn add_chunk(&self, data: Bytes) {
        let chunk = Arc::new(Chunk {
            data,
            next: Arc::new(ArcSwapOption::new(None)),
        });

        match self.current_chunk.load_full() {
            Some(last) => {
                last.next.store(Some(Arc::clone(&chunk)));
                self.current_chunk.store(Some(Arc::clone(&chunk)));
            }
            None => {
                self.start_chunk.store(Some(Arc::clone(&chunk)));
                self.current_chunk.store(Some(Arc::clone(&chunk)));
            }
        }

        self.notify.notify_waiters();
    }

    pub fn stream(self: Arc<Self>) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
        try_stream! {
            let mut current_chunk = Arc::clone(&self.start_chunk);

            loop {
                let notified = self.notify.notified();
                let closed = self.closed.load(Ordering::Acquire);

                if let Some(chunk) = current_chunk.load_full() {
                    current_chunk = Arc::clone(&chunk.next);

                    yield chunk.data.clone();
                    continue
                }

                if closed {
                    break
                }

                notified.await;
            }
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

pub struct SegmentGuard(pub Arc<Segment>);

impl Drop for SegmentGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}
