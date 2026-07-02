use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

use crate::segment::Segment;

pub struct AppState {
    pub segments: Arc<RwLock<BTreeMap<String, Arc<Segment>>>>
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            segments: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_app_state_default() {
        let state = AppState::default();

        assert_eq!(state.segments.read().await.len(), 0);
    }
}
