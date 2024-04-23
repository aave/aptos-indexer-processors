// Copyright © Aptos Foundation

use super::{
    counters::{
        CACHE_SIZE_IN_BYTES, FIRST_TRANSACTION_VERSION_IN_CACHE, LAST_TRANSACTION_VERSION_IN_CACHE,
    },
    stream::EventCacheKey,
};
use crate::models::events_models::events::CachedEvent;
use aptos_in_memory_cache::StreamableOrderedCache;
use futures::StreamExt;
use kanal::AsyncReceiver;
use std::sync::Arc;

pub struct CacheWriter<C: StreamableOrderedCache<EventCacheKey, CachedEvent> + 'static> {
    channel: AsyncReceiver<(EventCacheKey, CachedEvent)>,
    cache: Arc<C>,
}

impl<C: StreamableOrderedCache<EventCacheKey, CachedEvent> + 'static> CacheWriter<C> {
    pub fn new(channel: AsyncReceiver<(EventCacheKey, CachedEvent)>, cache: Arc<C>) -> Self {
        Self { channel, cache }
    }

    /// Maintains websocket connection and sends messages from channel
    pub async fn run(&mut self) {
        let cache = self.cache.clone();
        let mut channel = self.channel.stream();
        while let Some(cached_event) = channel.next().await {
            let txn_version = cached_event.0.transaction_version;
            cache.insert(cached_event.0, cached_event.1);
            LAST_TRANSACTION_VERSION_IN_CACHE.set(txn_version);
            FIRST_TRANSACTION_VERSION_IN_CACHE.set(
                cache
                    .first_key()
                    .map(|k| k.transaction_version)
                    .unwrap_or_default(),
            );
            CACHE_SIZE_IN_BYTES.set(cache.total_size() as i64);
        }
    }
}
