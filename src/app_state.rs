use dashmap::DashMap;
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::RwLock;

use crate::segment::Segment;

pub struct AppState {
    pub segments: DashMap<String, Arc<Segment>>,
    pub segments_list: Arc<RwLock<BTreeSet<String>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            segments: DashMap::new(),
            segments_list: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }
}
