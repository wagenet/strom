//! WHIP session manager for per-client whipserversrc elements.
//!
//! Each WHIP client session gets its own isolated GStreamer pipeline with a
//! whipserversrc. Media is bridged to the main pipeline via appsink→appsrc,
//! where each session is assigned to a numbered slot with independent output chains.
//!
//! Dead sessions (ICE disconnect, pipeline error) are automatically cleaned up
//! via a background task that receives cleanup requests through an mpsc channel.

mod activity;
mod endpoint;
mod takeover;
#[cfg(test)]
mod test_support;

use crate::blocks::DynamicWebrtcbinStore;
use gstreamer as gst;
use gstreamer::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub(crate) use activity::DECODE_GRACE;
pub use activity::{
    ActivityStamp, MediumBudgets, MissingMedium, SessionActivity, SlotMediumStall, SlotOutput,
    StallSide, WhipSlotLiveness,
};
pub use endpoint::{new_slot_activity, SessionCleanupRequest, SlotDecodebin, WhipEndpointConfig};

/// An active WHIP session (one whipserversrc element per client).
/// Each session runs in its own GStreamer pipeline to isolate NiceAgent instances.
struct WhipSession {
    /// Internal port where this session's whipserversrc is listening
    port: u16,
    /// The whipserversrc element for this session
    element: gst::Element,
    /// The isolated pipeline for this session's whipserversrc
    session_pipeline: gst::Pipeline,
    /// The endpoint this session belongs to
    endpoint_id: String,
    /// The slot index assigned to this session
    slot: usize,
    /// Set once the session is finished, by whichever path tears it down.
    /// Stops the session's inactivity watchdog thread and suppresses duplicate
    /// cleanup requests. Shared with the callbacks in `whip/session.rs`.
    cleanup_sent: Arc<AtomicBool>,
    /// When this session last delivered media. Shared with the session's appsink
    /// callbacks; see `SessionActivity`.
    activity: Arc<SessionActivity>,
}

/// One live session and the pipeline it runs in, for callers that need to
/// inspect a seat's receive path from outside the session manager.
pub struct WhipSessionPipeline {
    /// The resource_id the session was registered under
    pub resource_id: String,
    /// The slot this session occupies on its endpoint
    pub slot: usize,
    /// The session's own pipeline
    pub pipeline: gst::Pipeline,
}

/// A freshly created WHIP session, handed to `register_session`.
pub struct NewWhipSession {
    /// The resource_id assigned by the internal whipserversrc signaller
    pub resource_id: String,
    /// Internal port where this session's whipserversrc is listening
    pub port: u16,
    /// The whipserversrc element for this session
    pub element: gst::Element,
    /// The isolated pipeline for this session's whipserversrc
    pub session_pipeline: gst::Pipeline,
    /// The endpoint this session belongs to
    pub endpoint_id: String,
    /// The slot index assigned to this session
    pub slot: usize,
    /// The endpoint config `slot` was allocated from. `register_session`
    /// refuses the session unless this is still the config registered for
    /// `endpoint_id`: a POST still in flight when its flow stopped must not
    /// join the flow's next run under the same endpoint_id.
    pub config: Arc<WhipEndpointConfig>,
    /// Shared with the session's own callbacks; see `WhipSession::cleanup_sent`.
    pub cleanup_sent: Arc<AtomicBool>,
    /// Shared with the session's appsink callbacks; see `SessionActivity`.
    pub activity: Arc<SessionActivity>,
}

/// Manages WHIP sessions across all endpoints.
///
/// Thread-safe: uses RwLock for the sessions map and read-only Arc for endpoint configs.
pub struct WhipSessionManager {
    /// endpoint_id -> config (registered at pipeline start, immutable after that)
    endpoints: RwLock<HashMap<String, Arc<WhipEndpointConfig>>>,
    /// resource_id -> session (created/removed dynamically as clients connect/disconnect)
    sessions: RwLock<HashMap<String, WhipSession>>,
    /// Channel sender for cleanup requests from GStreamer callbacks
    cleanup_tx: mpsc::UnboundedSender<SessionCleanupRequest>,
    /// Channel receiver — taken once when starting the cleanup task
    cleanup_rx: Mutex<Option<mpsc::UnboundedReceiver<SessionCleanupRequest>>>,
    /// Ports for sessions that died before register_session was called, with the
    /// time they were marked. register_session checks this map and skips
    /// registration if the port is present and the mark has not expired.
    ///
    /// Marks expire after `PENDING_CLEANUP_TTL`: session ports come from the OS
    /// ephemeral range and are recycled, so a mark that is never claimed must not
    /// poison a later, unrelated session that happens to be given the same port.
    pending_cleanup_ports: Mutex<HashMap<u16, Instant>>,
}

/// How long a pending-cleanup mark stays valid. The window it has to cover is the
/// gap between a session dying and `register_session` running for it, which is
/// sub-second in practice.
const PENDING_CLEANUP_TTL: Duration = Duration::from_secs(30);

/// How long to wait for a session pipeline that returns ASYNC from its
/// transition to NULL. Bounded because the wait runs on a blocking thread.
const TEARDOWN_TIMEOUT: gst::ClockTime = gst::ClockTime::from_seconds(5);

impl WhipSessionManager {
    pub fn new() -> Self {
        let (cleanup_tx, cleanup_rx) = mpsc::unbounded_channel();
        Self {
            endpoints: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            cleanup_tx,
            cleanup_rx: Mutex::new(Some(cleanup_rx)),
            pending_cleanup_ports: Mutex::new(HashMap::new()),
        }
    }

    /// Get a clone of the cleanup channel sender.
    /// Pass this to `create_whipserversrc_for_session` so GStreamer callbacks can
    /// send cleanup requests.
    pub fn cleanup_sender(&self) -> mpsc::UnboundedSender<SessionCleanupRequest> {
        self.cleanup_tx.clone()
    }

    /// Start the background cleanup task.
    ///
    /// Receives cleanup requests from GStreamer callbacks and tears down dead sessions.
    /// Must be called once after the WhipSessionManager is created (from a tokio context).
    pub fn start_cleanup_task(self: &Arc<Self>) {
        let rx = self
            .cleanup_rx
            .lock()
            .unwrap()
            .take()
            .expect("start_cleanup_task called more than once");

        let manager = Arc::clone(self);
        tokio::spawn(async move {
            Self::run_cleanup_loop(manager, rx).await;
        });
        info!("WhipSessionManager: Cleanup task started");
    }

    async fn run_cleanup_loop(
        manager: Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<SessionCleanupRequest>,
    ) {
        while let Some(req) = rx.recv().await {
            info!(
                "WhipSessionManager: Auto-cleanup request for port {} (reason: {})",
                req.port, req.reason
            );

            // Try to find and remove the session by port
            let removed = manager.remove_session_by_port(req.port);

            match removed {
                Some((resource_id, element, session_pipeline, endpoint_id, _port, slot)) => {
                    // Release the slot
                    let webrtcbin_store =
                        if let Some(config) = manager.get_endpoint_config(&endpoint_id) {
                            config.release_slot(slot, &resource_id);
                            Some((
                                config.dynamic_webrtcbin_store.clone(),
                                config.instance_id.clone(),
                            ))
                        } else {
                            None
                        };

                    // Tear down session pipeline on a blocking thread.
                    // Keep element alive until after pipeline reaches NULL.
                    tokio::task::spawn_blocking(move || {
                        Self::teardown_session_pipeline(&session_pipeline);
                        drop(element);
                        // Remove stale webrtcbin entries so frontend stops showing dead stats
                        if let Some((store, block_id)) = webrtcbin_store {
                            Self::cleanup_dynamic_webrtcbin_store(&store, &block_id);
                        }
                    });

                    info!(
                        "WhipSessionManager: Auto-cleaned session '{}' for endpoint '{}' (slot {}, reason: {})",
                        resource_id, endpoint_id, slot, req.reason
                    );
                }
                None => {
                    // Session not registered yet (ICE failed before register_session).
                    // Mark port as pending cleanup so register_session skips it.
                    // The mark expires after PENDING_CLEANUP_TTL so it cannot poison
                    // a later session that is handed the same recycled port.
                    let mut pending = manager.pending_cleanup_ports.lock().unwrap();
                    pending.retain(|_, marked| marked.elapsed() < PENDING_CLEANUP_TTL);
                    pending.insert(req.port, Instant::now());
                    warn!(
                        "WhipSessionManager: Session on port {} not found, marked for pending cleanup (reason: {})",
                        req.port, req.reason
                    );
                }
            }
        }
        debug!("WhipSessionManager: Cleanup task exiting (channel closed)");
    }

    /// Register an endpoint configuration (called once per WHIP Input block at pipeline start).
    pub fn register_endpoint(&self, endpoint_id: String, config: WhipEndpointConfig) {
        info!(
            "WhipSessionManager: Registering endpoint '{}' (instance: {}, mode: {:?}, max_sessions: {})",
            endpoint_id, config.instance_id, config.mode, config.max_sessions
        );
        let mut endpoints = self.endpoints.write().unwrap();
        endpoints.insert(endpoint_id, Arc::new(config));
    }

    /// Get the endpoint configuration for creating new sessions.
    pub fn get_endpoint_config(&self, endpoint_id: &str) -> Option<Arc<WhipEndpointConfig>> {
        let endpoints = self.endpoints.read().unwrap();
        endpoints.get(endpoint_id).cloned()
    }

    /// Whether `config` is the one registered for its endpoint right now, as
    /// opposed to one left over from an earlier run of the flow.
    fn is_current_config(&self, config: &WhipEndpointConfig) -> bool {
        self.endpoints
            .read()
            .unwrap()
            .get(&config.endpoint_id)
            .is_some_and(|current| std::ptr::eq(Arc::as_ptr(current), config))
    }

    /// Register a new session after a whipserversrc has been created.
    ///
    /// If the session's port is in the pending_cleanup_ports set (ICE failed before
    /// registration), the session is immediately torn down instead of being registered.
    /// Returns true if registered, false if immediately cleaned up.
    ///
    /// The session is also refused when its endpoint is no longer registered, or
    /// has been registered again with a different config since the session's
    /// slot was allocated. That is a POST that was still in flight while its
    /// flow stopped: its slot belongs to a config nobody uses any more, and
    /// registering it under the flow's next run would let its watchdog release
    /// that run's slot of the same index from under a live publisher.
    pub fn register_session(&self, session: NewWhipSession) -> bool {
        let NewWhipSession {
            resource_id,
            port,
            element,
            session_pipeline,
            endpoint_id,
            slot,
            config,
            cleanup_sent,
            activity,
        } = session;

        let refuse = |why: &str| {
            // Nothing will tear this session down later, so stop its watchdog here.
            cleanup_sent.store(true, Ordering::SeqCst);
            warn!(
                "WhipSessionManager: Session '{}' on port {} for endpoint '{}' {}, tearing down immediately",
                resource_id, port, endpoint_id, why
            );
            // Release the slot on the config it was allocated from, never on
            // whatever is registered under the endpoint_id now.
            config.release_slot(slot, &resource_id);
            let pipeline = session_pipeline.clone();
            let element = element.clone();
            std::thread::spawn(move || {
                Self::teardown_session_pipeline(&pipeline);
                drop(element);
            });
            false
        };

        // Check if this port was marked for cleanup before we could register it.
        // The mark is taken under the lock; the refusal runs after it is dropped.
        let died_before_registration = {
            let mut pending = self.pending_cleanup_ports.lock().unwrap();
            pending.retain(|_, marked| marked.elapsed() < PENDING_CLEANUP_TTL);
            pending.remove(&port).is_some()
        };
        if died_before_registration {
            return refuse("died before registration");
        }

        // Held until the session is in the map, so `unregister_endpoint` either
        // runs first (and the session is refused here) or after (and sweeps it).
        let endpoints = self.endpoints.read().unwrap();
        match endpoints.get(&endpoint_id) {
            Some(current) if Arc::ptr_eq(current, &config) => {}
            Some(_) => {
                drop(endpoints);
                return refuse("belongs to an earlier run of its endpoint");
            }
            None => {
                drop(endpoints);
                return refuse("outlived its endpoint");
            }
        }

        info!(
            "WhipSessionManager: Registering session '{}' on port {} for endpoint '{}' (slot {})",
            resource_id, port, endpoint_id, slot
        );
        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(
            resource_id,
            WhipSession {
                port,
                element,
                session_pipeline,
                endpoint_id,
                slot,
                cleanup_sent,
                activity,
            },
        );
        true
    }

    /// The live sessions on an endpoint, with the pipeline each one runs in.
    ///
    /// A WHIP session's whipserversrc lives in its own pipeline, not the
    /// flow's, so inspecting a seat's receive path needs that pipeline. Sorted
    /// by slot so repeated polls return a stable order.
    pub fn sessions_for_endpoint(&self, endpoint_id: &str) -> Vec<WhipSessionPipeline> {
        let sessions = self.sessions.read().unwrap();
        let mut found: Vec<WhipSessionPipeline> = sessions
            .iter()
            .filter(|(_, s)| s.endpoint_id == endpoint_id)
            .map(|(resource_id, s)| WhipSessionPipeline {
                resource_id: resource_id.clone(),
                slot: s.slot,
                pipeline: s.session_pipeline.clone(),
            })
            .collect();
        found.sort_by_key(|s| s.slot);
        found
    }

    /// Look up the port for a session by resource_id.
    pub fn get_session_port(&self, resource_id: &str) -> Option<u16> {
        let sessions = self.sessions.read().unwrap();
        sessions.get(resource_id).map(|s| s.port)
    }

    /// Remove a session and return (element, session_pipeline, endpoint_id, port, slot) for teardown.
    pub fn remove_session(
        &self,
        resource_id: &str,
    ) -> Option<(gst::Element, gst::Pipeline, String, u16, usize)> {
        let mut sessions = self.sessions.write().unwrap();
        sessions.remove(resource_id).map(|s| {
            s.cleanup_sent.store(true, Ordering::SeqCst);
            (s.element, s.session_pipeline, s.endpoint_id, s.port, s.slot)
        })
    }

    /// Remove a session by its internal port (reverse lookup for auto-cleanup).
    /// Returns (resource_id, element, session_pipeline, endpoint_id, port, slot).
    fn remove_session_by_port(
        &self,
        port: u16,
    ) -> Option<(String, gst::Element, gst::Pipeline, String, u16, usize)> {
        let mut sessions = self.sessions.write().unwrap();
        let resource_id = sessions
            .iter()
            .find(|(_, s)| s.port == port)
            .map(|(k, _)| k.clone());

        if let Some(rid) = resource_id {
            sessions.remove(&rid).map(|s| {
                s.cleanup_sent.store(true, Ordering::SeqCst);
                (
                    rid,
                    s.element,
                    s.session_pipeline,
                    s.endpoint_id,
                    s.port,
                    s.slot,
                )
            })
        } else {
            None
        }
    }

    /// Remove all sessions for a given endpoint (called during pipeline stop).
    /// Returns (session_pipeline, element) pairs for teardown. The element must be
    /// kept alive until after the pipeline reaches NULL state.
    pub fn remove_all_sessions(&self, endpoint_id: &str) -> Vec<(gst::Pipeline, gst::Element)> {
        let mut sessions = self.sessions.write().unwrap();
        let resource_ids: Vec<String> = sessions
            .iter()
            .filter(|(_, s)| s.endpoint_id == endpoint_id)
            .map(|(k, _)| k.clone())
            .collect();

        let mut result = Vec::new();
        for resource_id in &resource_ids {
            if let Some(session) = sessions.remove(resource_id) {
                info!(
                    "WhipSessionManager: Removing session '{}' for endpoint '{}'",
                    resource_id, endpoint_id
                );
                session.cleanup_sent.store(true, Ordering::SeqCst);
                result.push((session.session_pipeline, session.element));
            }
        }
        result
    }

    /// Unregister an endpoint (called during pipeline stop, after
    /// `remove_all_sessions`).
    ///
    /// Returns any session that registered between `remove_all_sessions` and
    /// this call (a POST that was still in flight), removed and ready for
    /// teardown the same way. Once this returns, `register_session` refuses
    /// every session allocated from the endpoint's config.
    pub fn unregister_endpoint(&self, endpoint_id: &str) -> Vec<(gst::Pipeline, gst::Element)> {
        info!(
            "WhipSessionManager: Unregistering endpoint '{}'",
            endpoint_id
        );
        self.endpoints.write().unwrap().remove(endpoint_id);
        self.remove_all_sessions(endpoint_id)
    }

    /// Remove stale entries from the dynamic webrtcbin store for a block.
    ///
    /// After a session pipeline is set to NULL, its webrtcbin elements are dead
    /// but still referenced in the store (used for WebRTC stats in the frontend).
    /// This removes entries where the element is in NULL state.
    pub fn cleanup_dynamic_webrtcbin_store(store: &DynamicWebrtcbinStore, block_id: &str) {
        if let Ok(mut store) = store.lock() {
            if let Some(entries) = store.get_mut(block_id) {
                let before = entries.len();
                entries.retain(|(_, elem)| {
                    let (_, state, _) = elem.state(gst::ClockTime::ZERO);
                    state != gst::State::Null
                });
                let removed = before - entries.len();
                if removed > 0 {
                    debug!(
                        "WhipSessionManager: Removed {} stale webrtcbin entries for block '{}'",
                        removed, block_id
                    );
                }
            }
        }
    }

    /// Teardown a session's isolated pipeline.
    pub fn teardown_session_pipeline(session_pipeline: &gst::Pipeline) {
        let name = session_pipeline.name().to_string();
        debug!(
            "WhipSessionManager: Tearing down session pipeline '{}'",
            name
        );

        match session_pipeline.set_state(gst::State::Null) {
            // Nothing in a session pipeline goes async on the way down today,
            // but an element that did would have the pipeline dropped out from
            // under a transition still in flight, leaving its children to be
            // disposed above NULL.
            Ok(gst::StateChangeSuccess::Async) => {
                let (result, current, _) = session_pipeline.state(TEARDOWN_TIMEOUT);
                if result.is_err() || current != gst::State::Null {
                    warn!(
                        "WhipSessionManager: Session pipeline {} did not reach Null within {:?} (current: {:?})",
                        name, TEARDOWN_TIMEOUT, current
                    );
                }
            }
            Ok(_) => {}
            Err(e) => warn!(
                "WhipSessionManager: Failed to set session pipeline {} to Null: {:?}",
                name, e
            ),
        }
    }
}

impl Default for WhipSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::takeover::TAKEOVER_IDLE_THRESHOLD;
    use super::test_support::*;
    use super::*;

    #[test]
    fn remove_session_stops_the_watchdog() {
        let manager = WhipSessionManager::new();
        let cleanup_sent = register(&manager, "resource-a", 40001);

        assert!(manager.remove_session("resource-a").is_some());

        assert!(
            cleanup_sent.load(Ordering::SeqCst),
            "remove_session (WHIP DELETE) must stop the session's watchdog"
        );
    }

    #[test]
    fn remove_session_by_port_stops_the_watchdog() {
        let manager = WhipSessionManager::new();
        let cleanup_sent = register(&manager, "resource-b", 40002);

        assert!(manager.remove_session_by_port(40002).is_some());

        assert!(
            cleanup_sent.load(Ordering::SeqCst),
            "auto-cleanup by port must stop the session's watchdog"
        );
    }

    #[test]
    fn remove_all_sessions_stops_the_watchdog() {
        let manager = WhipSessionManager::new();
        let cleanup_sent = register(&manager, "resource-c", 40003);

        assert_eq!(manager.remove_all_sessions("endpoint").len(), 1);

        assert!(
            cleanup_sent.load(Ordering::SeqCst),
            "flow stop must stop the session's watchdog"
        );
    }

    /// A pending-cleanup mark that is never claimed must not survive: session ports
    /// come from the OS ephemeral range and are recycled, so a stale mark would
    /// destroy a later, unrelated session that is handed the same port.
    #[test]
    fn expired_pending_cleanup_mark_does_not_reject_a_recycled_port() {
        let manager = WhipSessionManager::new();
        {
            let mut pending = manager.pending_cleanup_ports.lock().unwrap();
            pending.insert(40004, Instant::now() - PENDING_CLEANUP_TTL * 2);
        }

        let (element, pipeline, cleanup_sent) = dummy_session();
        let registered = manager.register_session(NewWhipSession {
            resource_id: "resource-d".to_string(),
            port: 40004,
            element,
            session_pipeline: pipeline,
            endpoint_id: "endpoint".to_string(),
            slot: 0,
            config: registered_endpoint(&manager),
            cleanup_sent: cleanup_sent.clone(),
            activity: dead_publisher(Duration::ZERO),
        });

        assert!(
            registered,
            "an expired pending-cleanup mark must not poison a recycled port"
        );
        assert!(!cleanup_sent.load(Ordering::SeqCst));
    }

    /// The mark must still do its job inside the TTL: a session that died before
    /// registration is torn down rather than registered.
    #[test]
    fn fresh_pending_cleanup_mark_still_rejects_the_session() {
        let manager = WhipSessionManager::new();
        {
            let mut pending = manager.pending_cleanup_ports.lock().unwrap();
            pending.insert(40005, Instant::now());
        }

        let (element, pipeline, cleanup_sent) = dummy_session();
        let registered = manager.register_session(NewWhipSession {
            resource_id: "resource-e".to_string(),
            port: 40005,
            element,
            session_pipeline: pipeline,
            endpoint_id: "endpoint".to_string(),
            slot: 0,
            config: registered_endpoint(&manager),
            cleanup_sent: cleanup_sent.clone(),
            activity: dead_publisher(Duration::ZERO),
        });

        assert!(!registered, "a fresh mark must still reject the session");
        assert!(
            cleanup_sent.load(Ordering::SeqCst),
            "a rejected session's watchdog must be stopped too"
        );
    }

    /// A POST still in flight when its flow stopped must not register under
    /// the flow's next run, which re-registers the same endpoint_id with a new
    /// config. Registered, the orphan's watchdog would later release the new
    /// run's slot of the same index from under a live publisher.
    #[test]
    fn a_session_from_an_earlier_run_of_its_endpoint_is_refused() {
        let manager = WhipSessionManager::new();
        manager.register_endpoint("endpoint".to_string(), endpoint_config(1));
        let old_config = manager.get_endpoint_config("endpoint").unwrap();
        assert_eq!(old_config.allocate_slot("orphan"), Some(0));

        // The flow stops while the orphan's POST is still in flight...
        assert!(manager.remove_all_sessions("endpoint").is_empty());
        assert!(manager.unregister_endpoint("endpoint").is_empty());
        // ...and starts again, and a live publisher takes slot 0.
        manager.register_endpoint("endpoint".to_string(), endpoint_config(1));
        let new_config = manager.get_endpoint_config("endpoint").unwrap();
        assert_eq!(new_config.allocate_slot("live-publisher"), Some(0));

        let (element, pipeline, cleanup_sent) = dummy_session();
        let registered = manager.register_session(NewWhipSession {
            resource_id: "orphan".to_string(),
            port: 40020,
            element,
            session_pipeline: pipeline,
            endpoint_id: "endpoint".to_string(),
            slot: 0,
            config: old_config.clone(),
            cleanup_sent: cleanup_sent.clone(),
            activity: dead_publisher(Duration::ZERO),
        });

        assert!(
            !registered,
            "a session allocated from an earlier config must be refused"
        );
        assert!(
            cleanup_sent.load(Ordering::SeqCst),
            "a refused session's watchdog must be stopped"
        );
        assert!(manager.get_session_port("orphan").is_none());
        assert_eq!(
            new_config.slot_assignments.read().unwrap()[0].as_deref(),
            Some("live-publisher"),
            "the live publisher must keep its slot"
        );
        assert_eq!(
            old_config.slot_assignments.read().unwrap()[0],
            None,
            "the orphan's slot is released on the config it came from"
        );
    }

    /// A POST that fetched its config before the flow stopped, and is still in
    /// the takeover wait when the flow starts again, sees the old config full
    /// (stopping a flow does not free its slots). It must not displace a
    /// session of the new run, which shares the endpoint_id.
    #[tokio::test]
    async fn a_post_from_an_earlier_run_does_not_displace_a_new_runs_session() {
        let (manager, new_config, cleanup_sent) = full_endpoint(
            "new-run-session",
            40023,
            dead_publisher(TAKEOVER_IDLE_THRESHOLD * 2),
        );
        let old_config = endpoint_config(1);
        assert_eq!(old_config.allocate_slot("old-run-session"), Some(0));

        let slot = manager
            .allocate_slot_or_take_over(&old_config, "stale-post")
            .await;

        assert_eq!(slot, None, "a stale POST gets no slot");
        assert!(
            !cleanup_sent.load(Ordering::SeqCst),
            "the new run's session must not be displaced by a stale POST"
        );
        assert!(manager.get_session_port("new-run-session").is_some());
        assert_eq!(
            new_config.slot_assignments.read().unwrap()[0].as_deref(),
            Some("new-run-session")
        );
    }

    /// A POST still in flight when its flow stopped, and the flow has not
    /// started again.
    #[test]
    fn a_session_whose_endpoint_is_gone_is_refused() {
        let manager = WhipSessionManager::new();
        manager.register_endpoint("endpoint".to_string(), endpoint_config(1));
        let config = manager.get_endpoint_config("endpoint").unwrap();
        assert_eq!(config.allocate_slot("orphan"), Some(0));
        assert!(manager.unregister_endpoint("endpoint").is_empty());

        let (element, pipeline, cleanup_sent) = dummy_session();
        let registered = manager.register_session(NewWhipSession {
            resource_id: "orphan".to_string(),
            port: 40021,
            element,
            session_pipeline: pipeline,
            endpoint_id: "endpoint".to_string(),
            slot: 0,
            config,
            cleanup_sent: cleanup_sent.clone(),
            activity: dead_publisher(Duration::ZERO),
        });

        assert!(!registered, "a session must not outlive its endpoint");
        assert!(cleanup_sent.load(Ordering::SeqCst));
        assert!(manager.get_session_port("orphan").is_none());
    }

    /// Flow stop removes the endpoint's sessions, tears them down (which takes
    /// a while), then unregisters the endpoint. A POST that registers in
    /// between must be handed back by `unregister_endpoint` for teardown, not
    /// left registered against an endpoint that no longer exists.
    #[test]
    fn a_session_registered_during_flow_stop_is_swept_on_unregister() {
        let manager = WhipSessionManager::new();
        assert!(manager.remove_all_sessions("endpoint").is_empty());
        let cleanup_sent = register(&manager, "late-session", 40022);

        assert_eq!(
            manager.unregister_endpoint("endpoint").len(),
            1,
            "the late session must be handed back for teardown"
        );
        assert!(cleanup_sent.load(Ordering::SeqCst));
        assert!(manager.get_session_port("late-session").is_none());
    }
}
