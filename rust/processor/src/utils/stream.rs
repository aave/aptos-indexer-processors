// Copyright © Aptos Foundation

use crate::{
    models::events_models::events::CachedEvent,
    utils::{counters::GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS, filter::EventFilter},
};
use aptos_in_memory_cache::StreamableOrderedCache;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use std::{fmt::Debug, sync::Arc};
use tokio::sync::Notify;
use tracing::{info, warn};
use warp::filters::ws::{Message, WebSocket};

pub struct Stream<C: StreamableOrderedCache<EventCacheKey, CachedEvent> + 'static> {
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<EventFilter>,
    cache: Arc<C>,
    filter_edit_notify: Arc<Notify>,
}

impl<C: StreamableOrderedCache<EventCacheKey, CachedEvent> + 'static> Stream<C> {
    pub fn new(
        tx: SplitSink<WebSocket, Message>,
        filter: Arc<EventFilter>,
        cache: Arc<C>,
        filter_edit_notify: Arc<Notify>,
    ) -> Self {
        info!("Received WebSocket connection");
        Self {
            tx,
            filter,
            cache,
            filter_edit_notify,
        }
    }

    /// Maintains websocket connection and sends messages from channel
    pub async fn run(&mut self, starting_event: Option<EventCacheKey>) {
        let cache = self.cache.clone();
        let mut stream = Box::pin(cache.get_stream(starting_event));
        while let Some(cached_event) = stream.next().await {
            if self.filter.is_empty() {
                self.filter_edit_notify.notified().await;
            }

            let event = cached_event.event_stream_message;
            if self.filter.accounts.contains(&event.account_address)
                || self.filter.types.contains(&event.type_)
            {
                GRPC_TO_PROCESSOR_1_SERVE_LATENCY_IN_SECS.set({
                    use chrono::TimeZone;
                    let transaction_timestamp =
                        chrono::Utc.from_utc_datetime(&event.transaction_timestamp);
                    let transaction_timestamp = std::time::SystemTime::from(transaction_timestamp);
                    std::time::SystemTime::now()
                        .duration_since(transaction_timestamp)
                        .unwrap_or_default()
                        .as_secs_f64()
                });
                let msg = serde_json::to_string(&event).unwrap_or_default();
                if let Err(e) = self.tx.send(Message::text(msg)).await {
                    warn!(
                        error = ?e,
                        "[Event Stream] Failed to send message to WebSocket"
                    );
                    break;
                }
            }
        }

        if let Err(e) = self.tx.send(Message::text("Error starting stream")).await {
            eprintln!("Error sending error message: {:?}", e);
        }

        if let Err(e) = self.tx.send(Message::close()).await {
            eprintln!("Error sending close message: {:?}", e);
        }
    }
}

pub async fn spawn_stream<C: StreamableOrderedCache<EventCacheKey, CachedEvent> + 'static>(
    tx: SplitSink<WebSocket, Message>,
    filter: Arc<EventFilter>,
    cache: Arc<C>,
    starting_event: Option<EventCacheKey>,
    filter_edit_notify: Arc<Notify>,
) {
    let mut stream = Stream::new(tx, filter, cache, filter_edit_notify);
    stream.run(starting_event).await;
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct EventCacheKey {
    pub transaction_version: i64,
    pub event_index: i64,
}

impl EventCacheKey {
    pub fn new(transaction_version: i64, event_index: i64) -> Self {
        Self {
            transaction_version,
            event_index,
        }
    }
}
