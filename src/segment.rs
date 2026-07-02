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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_segment_single_chunk() {
        let segment = Arc::new(Segment::new());
        segment.add_chunk(Bytes::from("chunk"));

        let stream = segment.clone().stream();
        tokio::pin!(stream);

        assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("chunk"));
    }

    #[tokio::test]
    async fn test_segment_multiple_chunks() {
        let segment = Arc::new(Segment::new());
        segment.add_chunk(Bytes::from("first chunk"));
        segment.add_chunk(Bytes::from("second chunk"));

        let stream = segment.clone().stream();
        tokio::pin!(stream);

        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from("first chunk")
        );
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from("second chunk")
        );
    }

    #[tokio::test]
    async fn test_segment_closed_stops_stream() {
        let segment = Arc::new(Segment::new());
        segment.add_chunk(Bytes::from("chunk"));
        segment.close();

        let stream = segment.clone().stream();
        tokio::pin!(stream);

        assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("chunk"));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_segment_concurrent_readers() {
        let segment = Arc::new(Segment::new());
        segment.add_chunk(Bytes::from("first chunk"));
        segment.add_chunk(Bytes::from("second chunk"));
        segment.close();

        let segment_reader_1 = Arc::clone(&segment);
        let segment_reader_2 = Arc::clone(&segment);

        let segment_reader_task_1 = tokio::spawn(async move {
            let stream = segment_reader_1.stream();
            tokio::pin!(stream);

            let mut results = Vec::new();
            while let Some(Ok(chunk)) = stream.next().await {
                results.push(chunk);
            }
            results
        });

        let segment_reader_task_2 = tokio::spawn(async move {
            let stream = segment_reader_2.stream();
            tokio::pin!(stream);

            let mut results = Vec::new();
            while let Some(Ok(chunk)) = stream.next().await {
                results.push(chunk);
            }
            results
        });

        assert_eq!(segment_reader_task_1.await.unwrap().len(), 2);
        assert_eq!(segment_reader_task_2.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_segment_streaming() {
        let segment = Arc::new(Segment::new());
        let segment_clone = Arc::clone(&segment);

        let add_task = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            segment_clone.add_chunk(Bytes::from("chunk"));
        });

        let stream = segment.clone().stream();
        tokio::pin!(stream);

        assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from("chunk"));

        add_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_segment_guard_closes_on_drop() {
        let segment = Arc::new(Segment::new());
        let segment_clone = Arc::clone(&segment);

        segment.add_chunk(Bytes::from("test"));

        {
            let _segment_guard = SegmentGuard(Arc::clone(&segment_clone));
        }

        assert!(segment_clone.closed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_segment_large_data() {
        let segment = Arc::new(Segment::new());
        let large_data = Bytes::from(vec![0u8; 1024 * 1024]);

        segment.add_chunk(large_data.clone());

        let stream = segment.clone().stream();
        tokio::pin!(stream);

        let chunk = stream.next().await.unwrap().unwrap();

        assert_eq!(chunk.len(), 1024 * 1024);
    }
}
