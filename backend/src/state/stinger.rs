//! Running a stinger take.
//!
//! Resolution and validation live in `gst::stinger`; this is the half that
//! touches the pipeline: claiming the mixer, starting the overlay, landing the
//! transition beneath on the right frame, and tearing down afterwards.

use super::AppState;
use crate::blocks::builtin::html_graphic;
use crate::blocks::builtin::mediaplayer::{
    MediaPlayerKey, MediaPlayerState, MEDIA_PLAYER_REGISTRY,
};
use crate::gst::pipeline::PipelineError;
use gstreamer as gst;
use gstreamer::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strom_types::{FlowId, StromEvent};
use tracing::{debug, error, warn};

/// Holds a mixer's output thread while a stinger's take is applied. Dropping it
/// releases the mixer, so it must outlive the take.
struct CutGuard {
    _release: std::sync::mpsc::SyncSender<()>,
}

/// Where the overlay's time zero sits in the mixer's timeline, which is what
/// places the cut point on an output frame.
enum Anchor {
    /// The media player's bridge maps clip time onto the main pipeline once its
    /// first buffer after `play()` arrives.
    Clip(Arc<MediaPlayerState>),
    /// The first frame the page paints after the take. An idle page paints
    /// nothing, so that frame is the animation's first.
    Web(WebAnchor),
}

/// How soon after a take a page's first frame must arrive to be trusted as the
/// start of its animation, and the delivery delay assumed when it is not. See
/// `gst::stinger::web_stinger_start`.
const PAGE_FIRST_FRAME_GRACE: std::time::Duration = std::time::Duration::from_millis(70);
const PAGE_FIRST_FRAME_DELAY: std::time::Duration = std::time::Duration::from_millis(20);

struct WebAnchor {
    source: String,
    /// Weak, so a take in flight never keeps a stopped flow's pipeline alive.
    pad: gst::glib::WeakRef<gst::Pad>,
    probe: std::sync::Mutex<Option<gst::PadProbeId>>,
    created: std::time::Instant,
    /// Timestamp of the first buffer seen, and when it was seen (ns after
    /// `created`). Places wall time in the output's timeline.
    seen_pts: Arc<AtomicU64>,
    seen_at_ns: Arc<AtomicU64>,
    /// When the take changed the URL (ns after `created`).
    taken_at_ns: Arc<AtomicU64>,
    /// Timestamp of the first frame of new content after the take.
    first_pts: Arc<AtomicU64>,
    reported: std::sync::atomic::AtomicBool,
}

impl WebAnchor {
    /// Watch the block's output for the page's first new frame. Wait for
    /// [`WebAnchor::ready`] before changing the URL, then [`WebAnchor::taken`].
    fn watch(source: &str, pad: gst::Pad) -> Option<Self> {
        let created = std::time::Instant::now();
        let seen_pts = Arc::new(AtomicU64::new(u64::MAX));
        let seen_at_ns = Arc::new(AtomicU64::new(u64::MAX));
        let taken_at_ns = Arc::new(AtomicU64::new(u64::MAX));
        let first_pts = Arc::new(AtomicU64::new(u64::MAX));
        let last_memory = std::sync::atomic::AtomicUsize::new(0);
        let (p_seen_pts, p_seen_at, p_taken, p_first) = (
            seen_pts.clone(),
            seen_at_ns.clone(),
            taken_at_ns.clone(),
            first_pts.clone(),
        );
        // A per-buffer probe, which this codebase treats as performance
        // critical: a few atomic loads and compares per buffer, installed for
        // a single take and removed once the cut is placed.
        //
        // It watches the block's output, so it sees the timestamps the mixer
        // composites by; these need not be pipeline running time, since some
        // gstcefsrc builds stamp frames from a counter. The first buffer only
        // sets the baseline. After the take, only new content counts: a repeat
        // of the page's last frame, whether livesync's (flagged GAP) or a
        // cefsrc that re-sends its current frame, shares that frame's memory,
        // and a fresh paint is a new allocation.
        let probe = pad.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            let Some(buffer) = info.buffer() else {
                return gst::PadProbeReturn::Ok;
            };
            let memory = if buffer.n_memory() > 0 {
                buffer.peek_memory(0).as_ptr() as usize
            } else {
                0
            };
            let previous = last_memory.swap(memory, Ordering::Relaxed);
            let Some(pts) = buffer.pts().map(|t| t.nseconds()) else {
                return gst::PadProbeReturn::Ok;
            };
            if p_seen_pts.load(Ordering::Relaxed) == u64::MAX {
                p_seen_at.store(created.elapsed().as_nanos() as u64, Ordering::Relaxed);
                p_seen_pts.store(pts, Ordering::Relaxed);
                return gst::PadProbeReturn::Ok;
            }
            if p_taken.load(Ordering::Relaxed) == u64::MAX
                || buffer.flags().contains(gst::BufferFlags::GAP)
                || memory == previous
            {
                return gst::PadProbeReturn::Ok;
            }
            let _ = p_first.compare_exchange(u64::MAX, pts, Ordering::Relaxed, Ordering::Relaxed);
            gst::PadProbeReturn::Ok
        })?;
        Some(Self {
            source: source.to_string(),
            pad: pad.downgrade(),
            probe: std::sync::Mutex::new(Some(probe)),
            created,
            seen_pts,
            seen_at_ns,
            taken_at_ns,
            first_pts,
            reported: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Wait, up to `limit`, for the baseline buffer, so a page frame painted
    /// just after the take is never mistaken for it. The block's output never
    /// falls silent, so this is at most about a frame.
    async fn ready(&self, limit: std::time::Duration) {
        let until = std::time::Instant::now() + limit;
        while self.seen_pts.load(Ordering::Relaxed) == u64::MAX && std::time::Instant::now() < until
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// Record that the take has just changed the page's URL.
    fn taken(&self) {
        self.taken_at_ns
            .store(self.created.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Output timestamp at which the page's animation started, once known.
    fn offset_ns(&self) -> Option<i64> {
        use crate::gst::stinger::{web_stinger_start, WebStart};
        let taken_at = self.taken_at_ns.load(Ordering::Relaxed);
        if taken_at == u64::MAX {
            return None;
        }
        let taken_pts = match self.seen_pts.load(Ordering::Relaxed) {
            u64::MAX => None,
            seen_pts => {
                let seen_at = self.seen_at_ns.load(Ordering::Relaxed);
                let pts = seen_pts as i128 + taken_at as i128 - seen_at as i128;
                Some(pts.max(0) as u64)
            }
        };
        let first = match self.first_pts.load(Ordering::Relaxed) {
            u64::MAX => None,
            pts => Some(pts),
        };
        let elapsed = (self.created.elapsed().as_nanos() as u64).saturating_sub(taken_at);
        let start = web_stinger_start(
            first,
            taken_pts,
            elapsed,
            PAGE_FIRST_FRAME_GRACE.as_nanos() as u64,
            PAGE_FIRST_FRAME_DELAY.as_nanos() as u64,
        );
        let report = !self
            .reported
            .swap(!matches!(start, WebStart::Waiting), Ordering::Relaxed);
        let after_take = |ns: u64| ns.saturating_sub(taken_pts.unwrap_or(0)) / 1_000_000;
        match start {
            WebStart::Waiting => None,
            WebStart::FirstFrame(ns) => {
                if report {
                    debug!(
                        "HTML graphic {}: first frame {} ms after the take",
                        self.source,
                        after_take(ns)
                    );
                }
                i64::try_from(ns).ok()
            }
            WebStart::FromTake(ns) => {
                if report {
                    warn!(
                        "HTML graphic {}: no frame within {} ms of the take{}, so the cut is \
                         timed from the take and may be a frame out. A stinger page should \
                         change something visible on its first frame",
                        self.source,
                        PAGE_FIRST_FRAME_GRACE.as_millis(),
                        first
                            .map(|f| format!(" (first came after {} ms)", after_take(f)))
                            .unwrap_or_default()
                    );
                }
                i64::try_from(ns).ok()
            }
        }
    }

    fn stop_watching(&self) {
        if let Some(probe) = self.probe.lock().ok().and_then(|mut p| p.take()) {
            if let Some(pad) = self.pad.upgrade() {
                pad.remove_probe(probe);
            }
        }
    }
}

impl Drop for WebAnchor {
    fn drop(&mut self) {
        self.stop_watching();
    }
}

impl Anchor {
    fn offset_ns(&self) -> Option<i64> {
        match self {
            Anchor::Clip(player) => player.stream_offset_ns(),
            Anchor::Web(web) => web.offset_ns(),
        }
    }
}

/// The overlay a validated take will start, and how long it runs.
enum Overlay {
    Clip {
        player: Arc<MediaPlayerState>,
    },
    Web {
        cefsrc: gst::Element,
        output: gst::Pad,
        /// Page URL without a fragment; the take owns the fragment.
        base_url: String,
    },
}

/// What the spawned half of a take needs once validation has passed.
struct Take {
    flow: FlowId,
    mixer: String,
    source: String,
    dsk_index: usize,
    token: u64,
    cut_point_ms: u64,
    length_ms: u64,
    from_input: usize,
    to_input: usize,
    under: String,
    under_ms: u64,
    played_at: std::time::Instant,
    anchor: Anchor,
}

impl AppState {
    /// How long past the cut point to keep waiting for the overlay to reach the
    /// mixer before giving up on anchoring and cutting on wall clock.
    const STINGER_ANCHOR_GRACE: std::time::Duration = std::time::Duration::from_millis(500);
    const STINGER_ANCHOR_POLL: std::time::Duration = std::time::Duration::from_millis(2);
    /// Longest the mixer's output is held while a take is applied. Reaching it
    /// means the cut lands late, not that the mixer stays blocked.
    const STINGER_MAX_HOLD: std::time::Duration = std::time::Duration::from_millis(250);

    /// Trigger a stinger: play a keyed overlay over the program while another
    /// transition runs beneath it.
    ///
    /// Everything that can be rejected is rejected before anything moves on
    /// air, so a bad request leaves the program untouched. Once the overlay is
    /// running, a single task drives the rest: the underlying transition at the
    /// cut point, then teardown when the overlay ends.
    #[allow(clippy::too_many_arguments)]
    pub async fn trigger_stinger(
        &self,
        flow_id: &FlowId,
        block_instance_id: &str,
        from_input: usize,
        to_input: usize,
        source_block_id: Option<&str>,
    ) -> Result<String, PipelineError> {
        use crate::gst::stinger::{self, StingerError, StingerSourceKind};
        use crate::gst::transitions::TransitionType;

        // --- Validation. Nothing below this block touches the pipeline. ---
        let binding = {
            let flows = self.inner.flows.read().await;
            let flow = flows
                .get(flow_id)
                .ok_or_else(|| PipelineError::InvalidFlow(format!("No flow {flow_id}")))?;
            stinger::resolve_binding(
                &flow.blocks,
                &flow.links,
                block_instance_id,
                source_block_id,
            )?
        };

        // Cut point and the transition beneath are properties of the source,
        // not of the take: whoever made the artwork knows where it covers.
        let under_name = binding.under_transition.clone();
        let under_duration_ms = binding.under_duration_ms;
        let under_type: TransitionType = under_name
            .parse()
            .map_err(|_| StingerError::UnknownUnderTransition(under_name.clone()))?;
        if matches!(under_type, TransitionType::Stinger) {
            return Err(StingerError::StingerBeneathStinger.into());
        }

        // The overlay's length, whether it is ready, and how the take starts it.
        //
        // An overlay that cannot run degrades rather than rejects: a broken
        // source must not leave the program mid-transition. The mixer is not
        // claimed yet, so nothing to release.
        let (overlay, length_ms, armed) = match binding.kind {
            StingerSourceKind::Clip => {
                let key = MediaPlayerKey {
                    flow_id: *flow_id,
                    block_id: binding.source_block_id.clone(),
                };
                let player = MEDIA_PLAYER_REGISTRY
                    .get(&key)
                    .ok_or_else(|| StingerError::UnknownSource(binding.source_block_id.clone()))?;
                // No readable duration means the clip is missing or undecodable.
                let Some(clip_ms) = player
                    .duration()
                    .map(|ns| ns / 1_000_000)
                    .filter(|ms| *ms > 0)
                else {
                    return self
                        .cut_without_the_overlay(
                            flow_id,
                            block_instance_id,
                            &binding.source_block_id,
                            from_input,
                            to_input,
                            &under_name,
                            under_duration_ms,
                            "clip has no readable duration (missing or undecodable)",
                        )
                        .await;
                };
                let armed = player.is_stinger_armed();
                (Overlay::Clip { player }, clip_ms, armed)
            }
            StingerSourceKind::Web { duration_ms } => {
                let elements = {
                    let pipelines = self.inner.pipelines.read().await;
                    pipelines.get(flow_id).and_then(|m| {
                        let cefsrc = m.block_element(
                            &binding.source_block_id,
                            html_graphic::CEFSRC_ELEMENT,
                        )?;
                        let output = m
                            .block_element(&binding.source_block_id, html_graphic::OUTPUT_ELEMENT)?
                            .static_pad("src")?;
                        Some((cefsrc, output))
                    })
                };
                let Some((cefsrc, output)) = elements else {
                    return self
                        .cut_without_the_overlay(
                            flow_id,
                            block_instance_id,
                            &binding.source_block_id,
                            from_input,
                            to_input,
                            &under_name,
                            under_duration_ms,
                            "HTML graphic is not running",
                        )
                        .await;
                };
                let url = cefsrc.property::<Option<String>>("url").unwrap_or_default();
                let base_url = url.split('#').next().unwrap_or_default().to_string();
                let armed = cefsrc.current_state() == gst::State::Playing;
                (
                    Overlay::Web {
                        cefsrc,
                        output,
                        base_url,
                    },
                    duration_ms,
                    armed,
                )
            }
        };

        // A cut point is optional; without one, cut where a covering overlay is
        // most likely to be opaque.
        let cut_point = binding.cut_point_ms.unwrap_or(length_ms / 2);
        let (under_ms, clamped_from) =
            stinger::fit_under_transition(cut_point, under_duration_ms, length_ms)?;
        if let Some(requested) = clamped_from {
            warn!(
                "Stinger on {}: transition beneath shortened from {} ms to {} ms so it \
                 completes before the {} ms overlay ends",
                block_instance_id, requested, under_ms, length_ms
            );
        }

        if !armed {
            warn!(
                "Stinger source {} was not ready; its first frame will be late",
                binding.source_block_id
            );
        }

        // --- Claim the mixer. A stinger owns the program bus until it ends. ---
        let claim = (*flow_id, block_instance_id.to_string());
        let token = self
            .inner
            .next_stinger_token
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        {
            let mut in_flight = self.inner.stingers_in_flight.lock();
            if in_flight.contains_key(&claim) {
                return Err(StingerError::AlreadyRunning(block_instance_id.to_string()).into());
            }
            in_flight.insert(claim.clone(), token);
        }

        // From here on, every exit path must release the claim.
        let release = {
            let inner = self.inner.clone();
            let claim = claim.clone();
            move || {
                let mut in_flight = inner.stingers_in_flight.lock();
                if in_flight.get(&claim) == Some(&token) {
                    in_flight.remove(&claim);
                }
            }
        };

        if let Err(e) = self
            .set_dsk_enabled(flow_id, block_instance_id, binding.dsk_index, true)
            .await
        {
            release();
            return Err(e);
        }

        // Start the overlay. One that will not start costs the branding, not
        // the cut: run the transition beneath on its own so the program still
        // changes.
        let started = match overlay {
            Overlay::Clip { player } => player
                .play()
                .map(|()| Anchor::Clip(player))
                .map_err(|e| e.to_string()),
            Overlay::Web {
                cefsrc,
                output,
                base_url,
            } => match WebAnchor::watch(&binding.source_block_id, output) {
                Some(anchor) => {
                    anchor.ready(std::time::Duration::from_millis(100)).await;
                    // A new fragment is a same-document navigation: the loaded
                    // page gets hashchange and starts its animation, rather than
                    // reloading.
                    cefsrc.set_property("url", format!("{base_url}#strom-take-{token}"));
                    anchor.taken();
                    Ok(Anchor::Web(anchor))
                }
                None => Err("could not watch the HTML graphic's output".to_string()),
            },
        };
        let anchor = match started {
            Ok(anchor) => anchor,
            Err(reason) => {
                let _ = self
                    .set_dsk_enabled(flow_id, block_instance_id, binding.dsk_index, false)
                    .await;
                release();
                return self
                    .cut_without_the_overlay(
                        flow_id,
                        block_instance_id,
                        &binding.source_block_id,
                        from_input,
                        to_input,
                        &under_name,
                        under_ms,
                        &reason,
                    )
                    .await;
            }
        };

        self.inner.events.broadcast(StromEvent::StingerStarted {
            flow_id: *flow_id,
            block_instance_id: block_instance_id.to_string(),
            source_block_id: binding.source_block_id.clone(),
            clip_ms: length_ms,
            cut_point_ms: cut_point,
            under_transition: under_name.clone(),
            under_duration_ms: under_ms,
            under_duration_clamped_from: clamped_from,
            armed,
        });

        let state = self.clone();
        let take = Take {
            flow: *flow_id,
            mixer: block_instance_id.to_string(),
            source: binding.source_block_id.clone(),
            dsk_index: binding.dsk_index,
            token,
            cut_point_ms: cut_point,
            length_ms,
            from_input,
            to_input,
            under: under_name,
            under_ms,
            played_at: std::time::Instant::now(),
            anchor,
        };
        tokio::spawn(async move { state.run_take(take).await });

        Ok("stinger".to_string())
    }

    /// Run the transition beneath on its own, because the overlay will not run.
    ///
    /// A broken source costs the branding, not the cut: the program still
    /// changes rather than being left mid-transition.
    #[allow(clippy::too_many_arguments)]
    async fn cut_without_the_overlay(
        &self,
        flow_id: &FlowId,
        block_instance_id: &str,
        source_block_id: &str,
        from_input: usize,
        to_input: usize,
        under: &str,
        under_ms: u64,
        reason: &str,
    ) -> Result<String, PipelineError> {
        error!(
            "Stinger overlay on {}: {} — running the transition beneath on its own",
            source_block_id, reason
        );
        self.inner.events.broadcast(StromEvent::StingerFailed {
            flow_id: *flow_id,
            block_instance_id: block_instance_id.to_string(),
            source_block_id: source_block_id.to_string(),
            reason: reason.to_string(),
            still_running: false,
        });
        self.trigger_transition(
            flow_id,
            block_instance_id,
            from_input,
            to_input,
            under,
            under_ms,
        )
        .await
    }

    /// Land the transition beneath at the cut point, then tear the stinger down
    /// when the overlay ends.
    async fn run_take(&self, take: Take) {
        let Take {
            flow,
            mixer,
            source,
            dsk_index,
            token,
            cut_point_ms: cut_point,
            length_ms,
            from_input,
            to_input,
            under: under_name,
            under_ms,
            played_at,
            anchor,
        } = take;
        let state = self;
        let claim = (flow, mixer.clone());
        let still_ours = || state.inner.stingers_in_flight.lock().get(&claim) == Some(&token);

        // Hold the mixer on the frame before the one carrying the cut
        // point, so the take is applied before that frame is composited.
        // Dropping the guard releases it.
        let guard = state
            .hold_mixer_before_cut(&flow, &mixer, &anchor, cut_point, played_at)
            .await;
        if let Anchor::Web(web) = &anchor {
            web.stop_watching();
        }
        if guard.is_none() {
            // Nothing to anchor to: fall back to wall clock from the take.
            let elapsed = played_at.elapsed();
            let remaining = std::time::Duration::from_millis(cut_point).saturating_sub(elapsed);
            tokio::time::sleep(remaining).await;
        }

        // The flow may have been stopped and restarted while this waited.
        // Acting now would drive a pipeline this take knows nothing about.
        if !still_ours() {
            debug!(
                "Stinger on {} was superseded; leaving the mixer alone",
                mixer
            );
            return;
        }

        let beneath = state
            .trigger_transition(&flow, &mixer, from_input, to_input, &under_name, under_ms)
            .await;
        // Release the mixer only once the take has been applied.
        drop(guard);
        if let Err(e) = beneath {
            // The overlay is already on air, so this is the worst place to
            // fail quietly: the graphic plays but the program never
            // changes. Report it rather than leaving it in the log.
            error!("Stinger on {}: transition beneath failed: {}", mixer, e);
            state.inner.events.broadcast(StromEvent::StingerFailed {
                flow_id: flow,
                block_instance_id: mixer.clone(),
                source_block_id: source.clone(),
                reason: format!("the transition beneath did not run: {e}"),
                still_running: true,
            });
        }

        // Hold the keyed pad up for the rest of the overlay.
        let remaining = length_ms.saturating_sub(cut_point);
        tokio::time::sleep(std::time::Duration::from_millis(remaining)).await;

        if !still_ours() {
            debug!(
                "Stinger on {} was superseded; leaving the mixer alone",
                mixer
            );
            return;
        }

        if let Err(e) = state.set_dsk_enabled(&flow, &mixer, dsk_index, false).await {
            error!(
                "Stinger on {}: could not hide the keyed input: {}",
                mixer, e
            );
        }

        // Re-arm a clip so the next fire is fast again. A page resets itself.
        if matches!(anchor, Anchor::Clip(_)) {
            if let Some(player) = MEDIA_PLAYER_REGISTRY.get(&MediaPlayerKey {
                flow_id: flow,
                block_id: source.clone(),
            }) {
                if let Err(e) = player.arm_stinger() {
                    warn!("Stinger source {} could not be re-armed: {}", source, e);
                }
            }
        }
        {
            let mut in_flight = state.inner.stingers_in_flight.lock();
            if in_flight.get(&claim) == Some(&token) {
                in_flight.remove(&claim);
            }
        }
        state.inner.events.broadcast(StromEvent::StingerCompleted {
            flow_id: flow,
            block_instance_id: mixer.clone(),
            source_block_id: source.clone(),
        });
        debug!("Stinger on {} complete", mixer);
    }

    /// Hold the mixer's output thread just before the frame that carries the
    /// cut point, returning a guard whose drop releases it.
    ///
    /// A cut point is a position in the overlay, but a take is applied by
    /// setting pad properties from another thread, which races the aggregator:
    /// whether the change reaches the frame it was meant for depends on how the
    /// mixer happened to be scheduled. Waiting on wall clock or on the mixer's
    /// reported position both land a frame out, and by different amounts on
    /// different layouts, because a mixer whose inputs all have data aggregates
    /// as soon as it can while one waiting on a live source runs to its
    /// deadline.
    ///
    /// Anchoring removes the race. The anchor says where the overlay's time zero
    /// sits in the mixer's timeline, which gives the output frame whose interval
    /// contains the cut point. Blocking the mixer's src pad on the frame before
    /// it means the take is always applied first.
    ///
    /// Returns `None` when there is nothing to anchor to, leaving the caller to
    /// fall back to wall clock.
    async fn hold_mixer_before_cut(
        &self,
        flow_id: &FlowId,
        block_instance_id: &str,
        anchor: &Anchor,
        cut_point_ms: u64,
        played_at: std::time::Instant,
    ) -> Option<CutGuard> {
        let give_up =
            played_at + std::time::Duration::from_millis(cut_point_ms) + Self::STINGER_ANCHOR_GRACE;

        // The offset is only known once the overlay's first frame has arrived.
        let offset = loop {
            if let Some(o) = anchor.offset_ns() {
                break o;
            }
            if std::time::Instant::now() > give_up {
                warn!(
                    "Stinger on {}: overlay never delivered a first frame, cutting on \
                     wall clock",
                    block_instance_id
                );
                return None;
            }
            tokio::time::sleep(Self::STINGER_ANCHOR_POLL).await;
        };

        let (frame_ns, pad) = {
            let pipelines = self.inner.pipelines.read().await;
            let manager = pipelines.get(flow_id)?;
            (
                manager.mixer_frame_duration_ns(block_instance_id)?,
                manager.mixer_src_pad(block_instance_id)?,
            )
        };

        // Floor, not round: the frame that carries the cut point is the one
        // whose interval contains it, and rounding up is a frame late.
        let cut_at = offset.saturating_add((cut_point_ms as i64).saturating_mul(1_000_000));
        if cut_at < 0 {
            return None;
        }
        let cut_frame_pts = (cut_at as u64 / frame_ns) * frame_ns;
        // Half a frame back from the preceding frame, so a pts landing slightly
        // off the grid still matches it rather than the cut frame itself.
        let hold_from_pts = cut_frame_pts
            .saturating_sub(frame_ns)
            .saturating_sub(frame_ns / 2);

        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel::<u64>();
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel::<()>(0);
        let reached_tx = std::sync::Mutex::new(Some(reached_tx));
        let release_rx = std::sync::Mutex::new(release_rx);
        let fired = std::sync::atomic::AtomicBool::new(false);
        let held_for = Self::STINGER_MAX_HOLD;
        let probe_mixer = block_instance_id.to_string();

        // A per-buffer probe, which this codebase treats as performance
        // critical. Every buffer before the target costs one timestamp compare;
        // the lock and the channel run once, on the buffer it holds, and the
        // probe removes itself there. It is installed for a single take.
        let probe = pad.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            let Some(pts) = info.buffer().and_then(|b| b.pts()) else {
                return gst::PadProbeReturn::Ok;
            };
            if pts.nseconds() < hold_from_pts
                || fired.swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                return gst::PadProbeReturn::Ok;
            }
            if let Some(tx) = reached_tx.lock().ok().and_then(|mut g| g.take()) {
                let _ = tx.send(pts.nseconds());
            }
            let waited = std::time::Instant::now();
            let outcome = release_rx.lock().ok().map(|rx| rx.recv_timeout(held_for));
            if matches!(
                outcome,
                Some(Err(std::sync::mpsc::RecvTimeoutError::Timeout))
            ) {
                warn!(
                    "Stinger on {}: took longer than {} ms to apply, so the cut may \
                     land a frame late",
                    probe_mixer,
                    held_for.as_millis()
                );
            }
            debug!(
                "Stinger on {}: held the mixer for {:?}",
                probe_mixer,
                waited.elapsed()
            );
            gst::PadProbeReturn::Remove
        })?;

        match tokio::time::timeout(
            give_up.saturating_duration_since(std::time::Instant::now()),
            reached_rx,
        )
        .await
        {
            Ok(Ok(_)) => Some(CutGuard {
                _release: release_tx,
            }),
            _ => {
                // Never reached: drop the sender so the probe does not hold the
                // mixer when it eventually fires, and cut on wall clock.
                pad.remove_probe(probe);
                warn!(
                    "Stinger on {}: mixer never reached the cut frame, cutting on \
                     wall clock",
                    block_instance_id
                );
                None
            }
        }
    }
}
