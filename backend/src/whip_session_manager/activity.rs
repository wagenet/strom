//! Session liveness: the activity stamps and how a session is judged idle.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};
use strom_types::block::StreamMode;
use strom_types::flow::{BlockHealthCause, HealthMedium, MediumFault};

/// One stream of buffers, reduced to when its first and most recent buffer went
/// past, as milliseconds from a fixed epoch.
///
/// Written from GStreamer's data path — an appsink callback, a pad probe — so
/// `touch` is one clock read (`Instant` takes the vDSO fast path) plus relaxed
/// atomics: no lock, no allocation, no formatting. 0 means "nothing yet", so a
/// buffer landing inside the first millisecond counts as 1.
pub struct ActivityStamp {
    epoch: Instant,
    first_ms: AtomicU64,
    last_ms: AtomicU64,
    /// The first buffer of the current run: one that came after a gap of at
    /// least `ARRIVAL_PAUSE`, or the very first. Only meaningful on a stamp
    /// with a single writer; see `since_run_start`.
    run_ms: AtomicU64,
}

/// A gap in a stream at least this long ends a run of buffers; see
/// `ActivityStamp::since_run_start`.
///
/// Longer than the gaps a stream that is still flowing leaves: Opus DTX sends
/// a packet every 400 ms through silence. A stream sparser than this (video
/// below about 1 fps) starts a new run on every buffer, so it is never judged
/// on its own; see `medium_stall`.
pub(crate) const ARRIVAL_PAUSE: Duration = Duration::from_secs(1);

impl ActivityStamp {
    pub fn new(epoch: Instant) -> Self {
        Self {
            epoch,
            first_ms: AtomicU64::new(0),
            last_ms: AtomicU64::new(0),
            run_ms: AtomicU64::new(0),
        }
    }

    /// Stamp a buffer. Per-buffer hot path; see the type comment.
    pub fn touch(&self) {
        let ms = (self.epoch.elapsed().as_millis() as u64).max(1);
        // A load and a store, not a swap: no read-modify-write on the hot
        // path. Two writers can race here and misplace `run_ms`, which only
        // stamps with a single writer are ever asked about.
        let previous = self.last_ms.load(Ordering::Relaxed);
        self.last_ms.store(ms, Ordering::Relaxed);
        if previous == 0 || ms.saturating_sub(previous) >= ARRIVAL_PAUSE.as_millis() as u64 {
            self.run_ms.store(ms, Ordering::Relaxed);
        }
        // Only the very first buffer writes `first_ms`; every later one pays a
        // relaxed load and a branch that predicts perfectly.
        if self.first_ms.load(Ordering::Relaxed) == 0 {
            let _ = self
                .first_ms
                .compare_exchange(0, ms, Ordering::Relaxed, Ordering::Relaxed);
        }
    }

    /// Forget everything seen so far. Used when a new session claims a slot
    /// whose output chain outlives individual sessions.
    pub fn reset(&self) {
        self.first_ms.store(0, Ordering::Relaxed);
        self.last_ms.store(0, Ordering::Relaxed);
        self.run_ms.store(0, Ordering::Relaxed);
    }

    /// Milliseconds from `epoch` at which the most recent buffer went past,
    /// 0 if none has. Only meaningful compared against another reading of the
    /// same stamp — a value that changed between two of them is a stream that
    /// is still moving.
    pub fn last(&self) -> u64 {
        self.last_ms.load(Ordering::Relaxed)
    }

    /// Time since the most recent buffer, `None` if none has gone past.
    pub fn since_last(&self) -> Option<Duration> {
        self.since(self.last_ms.load(Ordering::Relaxed))
    }

    /// Time since the first buffer, `None` if none has gone past.
    pub fn since_first(&self) -> Option<Duration> {
        self.since(self.first_ms.load(Ordering::Relaxed))
    }

    /// Time since the first buffer of the current run, `None` if none has
    /// gone past. A stream that pauses for `ARRIVAL_PAUSE` or longer starts a
    /// new run when it resumes, so this says how long it has been flowing
    /// without a break.
    pub fn since_run_start(&self) -> Option<Duration> {
        self.since(self.run_ms.load(Ordering::Relaxed))
    }

    /// A stamp that already looks as if its first buffer went past
    /// `since_first` ago and its most recent one `since_last` ago, so a test can
    /// stand a session up mid-life without waiting out real seconds.
    #[cfg(test)]
    pub fn backdated(since_first: Duration, since_last: Duration) -> Self {
        assert!(
            since_last <= since_first,
            "the first buffer cannot be newer than the last"
        );
        // 1 ms of headroom keeps `first_ms` clear of the 0 that means "nothing
        // yet", exactly as `touch` does.
        let stamp = Self::new(Instant::now() - since_first - Duration::from_millis(1));
        stamp.first_ms.store(1, Ordering::Relaxed);
        stamp.run_ms.store(1, Ordering::Relaxed);
        stamp.last_ms.store(
            (since_first - since_last).as_millis() as u64 + 1,
            Ordering::Relaxed,
        );
        stamp
    }

    fn since(&self, ms: u64) -> Option<Duration> {
        if ms == 0 {
            return None;
        }
        Some(
            self.epoch
                .elapsed()
                .saturating_sub(Duration::from_millis(ms)),
        )
    }
}

/// What comes out of one slot's chains, one stamp per medium.
///
/// Each stamp is written by a probe on that medium's output tee. They are kept
/// apart because a slot can lose one medium and keep the other: a video decoder
/// that wedges while audio keeps flowing would otherwise look live forever.
pub struct SlotOutput {
    pub audio: Arc<ActivityStamp>,
    pub video: Arc<ActivityStamp>,
}

impl SlotOutput {
    pub fn new(epoch: Instant) -> Self {
        Self {
            audio: Arc::new(ActivityStamp::new(epoch)),
            video: Arc::new(ActivityStamp::new(epoch)),
        }
    }

    /// A slot whose audio stamp is `audio` and whose video has produced nothing.
    #[cfg(test)]
    pub fn with_audio(audio: ActivityStamp) -> Self {
        Self {
            audio: Arc::new(audio),
            video: Arc::new(ActivityStamp::new(Instant::now())),
        }
    }

    /// Forget both media; see `ActivityStamp::reset`.
    pub fn reset(&self) {
        self.audio.reset();
        self.video.reset();
    }

    /// A counter that changes whenever either medium produces a buffer. Only
    /// meaningful compared against another reading of it.
    pub fn last(&self) -> u64 {
        self.audio.last().wrapping_add(self.video.last())
    }

    /// Time since either medium last produced a buffer, `None` if neither has.
    pub fn since_last(&self) -> Option<Duration> {
        match (self.audio.since_last(), self.video.since_last()) {
            (Some(a), Some(v)) => Some(a.min(v)),
            (a, v) => a.or(v),
        }
    }
}

/// How long a slot's video may stay frozen while its frames keep arriving
/// before it counts against the session.
///
/// A damaged stream is repaired with a keyframe, and the session asks the
/// publisher for one once a second, up to ten times; see
/// `keyframe_request::RecoveryPolicy`. A seat must not be displaced while that
/// repair still has a chance. Past it, the decoder is wedged and the seat
/// shows a frozen picture for as long as its audio flows.
pub(crate) const VIDEO_REPAIR_WINDOW: Duration = Duration::from_secs(10);

/// How long one medium has kept arriving at a session while nothing of it came
/// out of the slot's chain, `None` if it must not be judged.
///
/// `None` when the medium never arrived (the publisher did not negotiate it,
/// or has not sent it yet) or only started arriving less than `DECODE_GRACE`
/// ago. Otherwise the time from the first buffer in that nothing came out
/// after to the last one in:
/// - A medium the publisher stopped sending (a camera turned off, a screen
///   share of a window nobody touches) stops adding to it.
/// - A medium that resumes after a pause of `ARRIVAL_PAUSE` or more is judged
///   from when it resumed, not from the last buffer that came out before the
///   pause: the pause was the publisher's, not a stall.
/// - Output stamped before this medium's grace ran out (the previous
///   occupant's trailing frames) is no older than the grace.
fn medium_stall(ingress: &ActivityStamp, output: &ActivityStamp) -> Option<Duration> {
    let ingress_idle = ingress.since_last()?;
    let past_grace = ingress.since_first()?.checked_sub(DECODE_GRACE)?;
    let run = ingress.since_run_start()?;
    let output_idle = output
        .since_last()
        .map_or(past_grace, |idle| idle.min(past_grace));
    Some(output_idle.min(run).saturating_sub(ingress_idle))
}

/// Which of a session's two stamps stopped moving; see `SessionActivity::idle`.
///
/// The two are different faults with different suspects, and a reap message that
/// says only "no usable media" gives an operator no way to tell a participant
/// whose laptop closed from a blocked consumer inside their own flow that is
/// costing every publisher its seat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallSide {
    /// Nothing is arriving from the publisher any more. The suspect is the
    /// client or its transport.
    Ingress,
    /// Media is still arriving, but nothing usable is leaving the slot's chain.
    /// The suspect is inside the flow: a decoder that never got its keyframe, or
    /// a consumer downstream of the slot's tee blocking and backing pressure up.
    Output,
    /// One medium is still arriving, but nothing of it is leaving the slot's
    /// chain, whatever the other medium does. Named so a reap of a seat whose
    /// audio still played says it was the video.
    MediumOutput(&'static str),
}

impl std::fmt::Display for StallSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StallSide::Ingress => f.write_str("nothing arriving from the publisher"),
            StallSide::Output => {
                f.write_str("still receiving, nothing usable leaving the slot's chain")
            }
            StallSide::MediumOutput(medium) => write!(
                f,
                "still receiving {medium}, none of it leaving the slot's chain"
            ),
        }
    }
}

/// Liveness of one WHIP session, in the only terms that matter to the slot it
/// occupies: is it still producing media the flow can use?
///
/// Two kinds of stamp decide it, because arriving bytes are not the same thing
/// as usable media:
///
/// - `ingress` is stamped by the session pipeline's appsink, once per buffer
///   that crosses the appsink→appsrc bridge. It says the publisher is still
///   sending, and nothing more. `ingress_audio` and `ingress_video` count the
///   same arrivals per medium.
/// - `output` is stamped by a pad probe on each of the slot's output tees, in
///   the *main* pipeline, downstream of the slot's `decodebin`. It says frames
///   are coming out the far end and reaching the flow's consumers.
///
/// A seat can sit at the first without the second indefinitely — a decoder that
/// never gets the keyframe it needs, or a downstream consumer that blocks and
/// backs pressure up through the slot's tee — and by `ingress` alone it looks
/// perfectly healthy while producing nothing. The same holds for one medium
/// while the other flows: a video decoder that wedges leaves a frozen picture
/// behind audio that keeps playing, so each medium is judged on its own too.
///
/// Read by the session's inactivity watchdog and by
/// `allocate_slot_or_take_over`.
pub struct SessionActivity {
    ingress: ActivityStamp,
    /// The same arrivals, counting audio buffers only. Shares `ingress`'s epoch.
    /// Non-zero means this session negotiated an audio stream and it produced
    /// something, which is what makes `TAKEOVER_IDLE_THRESHOLD` a sound bound;
    /// see `has_delivered_audio`.
    ingress_audio: ActivityStamp,
    /// The same arrivals, counting video buffers only. Shares `ingress`'s epoch.
    ingress_video: ActivityStamp,
    /// Shared with the slot, not owned by the session: the slot's output chain
    /// is built once, at flow build time, and outlives the sessions that pass
    /// through it. `SessionActivity::new` resets it so one session never
    /// inherits its predecessor's liveness.
    output: Arc<SlotOutput>,
}

impl SessionActivity {
    /// `epoch` is session start; `output` is the stamps belonging to the slot
    /// this session was assigned.
    pub fn new(epoch: Instant, output: Arc<SlotOutput>) -> Self {
        // This session has to prove for itself that its media comes out of the
        // slot's chain. Frames already in flight from the previous occupant can
        // still stamp it; `idle` holds the decode grace so they cannot count
        // against it.
        output.reset();
        Self {
            ingress: ActivityStamp::new(epoch),
            ingress_audio: ActivityStamp::new(epoch),
            ingress_video: ActivityStamp::new(epoch),
            output,
        }
    }

    /// Assemble a session from stamps a test has already positioned in time.
    /// Skips the reset `new` does, which would wipe them. The session counts
    /// as carrying audio from now on, so it is in displacement range while its
    /// audio is still inside its own decode grace, and it has carried no video.
    #[cfg(test)]
    pub fn from_stamps(ingress: ActivityStamp, output: Arc<SlotOutput>) -> Self {
        let ingress_audio = ActivityStamp::new(Instant::now());
        ingress_audio.touch();
        Self {
            ingress,
            ingress_audio,
            ingress_video: ActivityStamp::new(Instant::now()),
            output,
        }
    }

    /// Assemble a session that has never carried audio, so nothing about it can
    /// be judged on a short silence; see `has_delivered_audio`.
    #[cfg(test)]
    pub fn video_only_from_stamps(ingress: ActivityStamp, output: Arc<SlotOutput>) -> Self {
        Self {
            ingress,
            ingress_audio: ActivityStamp::new(Instant::now()),
            ingress_video: ActivityStamp::new(Instant::now()),
            output,
        }
    }

    /// Assemble a session from every stamp, each already positioned in time.
    #[cfg(test)]
    pub fn from_media_stamps(
        ingress: ActivityStamp,
        ingress_audio: ActivityStamp,
        ingress_video: ActivityStamp,
        output: Arc<SlotOutput>,
    ) -> Self {
        Self {
            ingress,
            ingress_audio,
            ingress_video,
            output,
        }
    }

    /// Stamp the arrival of a buffer from the publisher. Called from the
    /// appsink callback, so this is the per-buffer hot path.
    pub fn touch_ingress(&self, is_audio: bool) {
        self.ingress.touch();
        if is_audio {
            self.ingress_audio.touch();
        } else {
            self.ingress_video.touch();
        }
    }

    /// Whether this session has ever carried an audio buffer.
    ///
    /// Only an audio-bearing session can be judged dead on a couple of seconds
    /// of silence, because only audio has a cadence tight enough for a gap that
    /// short to mean anything: ~20 ms per packet, against a video stream whose
    /// interval is whatever the publisher's encoder decided to emit. A
    /// video-only publisher of static content — a screen share of a window
    /// nobody is touching — legitimately goes seconds between frames on a
    /// perfectly healthy transport, and is indistinguishable from a dead one by
    /// any media-based signal. Such a session is left to the inactivity
    /// watchdog; see `allocate_slot_or_take_over`.
    ///
    /// Read on the arriving side, not the slot's output: the question is what
    /// the publisher negotiated.
    pub fn has_delivered_audio(&self) -> bool {
        self.ingress_audio.last() != 0
    }

    /// The slot's output counter, for comparing two readings a poll apart. A
    /// value that changed is a session that is still producing something;
    /// whether it produces every medium it sends is for `idle` to say.
    pub fn last_usable(&self) -> u64 {
        self.output.last()
    }

    /// Time since this session last produced usable media, or `None` if it must
    /// not be judged yet.
    ///
    /// `None` means one of two states that must both be left alone: nothing has
    /// arrived at all (still negotiating — ICE through a TURN relay is slow, and
    /// evicting it lets two clients take turns throwing each other off before
    /// either sends media), or the first buffers arrived less than
    /// `DECODE_GRACE` ago and the decoder may still be waiting for the keyframe
    /// that carries H.264's parameter sets.
    ///
    /// The grace holds even if the output stamps have moved. After a takeover
    /// the displaced session's frames already inside the slot's chain still
    /// cross the tee after `new` reset the stamps, and those must not start the
    /// clock on a newcomer whose own decoder has not produced anything yet.
    /// The grace covers that tail only while it is short: nothing flushes the
    /// slot's appsrc, so a chain that had fallen behind can drain up to
    /// `APPSRC_MAX_TIME` of the predecessor's media, and a newcomer that
    /// decodes nothing reads as live until it ends.
    ///
    /// Otherwise it is the stalest of:
    /// - `ingress` against the slot's output as a whole: a session is usable
    ///   only while both move, and a publisher going away freezes `ingress`
    ///   first while a stall below the decoder freezes the output first.
    /// - Each medium the publisher sends, judged on its own by `medium_stall`:
    ///   how long it kept arriving while nothing of it came out. Video is
    ///   allowed `VIDEO_REPAIR_WINDOW` first, the time keyframe repair takes to
    ///   give up. Without this a seat whose video died holds its slot for as
    ///   long as its audio flows.
    ///
    /// The `StallSide` that comes with it is for the reap log only and never
    /// changes the duration.
    pub fn idle(&self) -> Option<(Duration, StallSide)> {
        let ingress_idle = self.ingress.since_last()?;
        let past_grace = self.ingress.since_first()?.checked_sub(DECODE_GRACE)?;

        let output_idle = match self.output.since_last() {
            Some(idle) => idle,
            // Media is arriving but nothing has come out of the decode chain.
            // Past the grace this is the failure the output stamp exists to
            // catch, and the session has been useless since the grace ran out.
            None => past_grace,
        };

        // A publisher that goes away freezes `ingress` while the last buffers
        // are still draining through the slot, so it is the staler one and
        // anything close to a tie belongs to it. Only `output` being clearly
        // staler means media is still arriving, which is the case worth naming.
        let side = if output_idle > ingress_idle + STALL_SIDE_MARGIN {
            StallSide::Output
        } else {
            StallSide::Ingress
        };
        let mut idle = (ingress_idle.max(output_idle), side);

        // A medium that keeps arriving while nothing of it comes out is a stall
        // inside the flow, whatever the other medium is doing.
        let media = [
            (
                &self.ingress_audio,
                &self.output.audio,
                Duration::ZERO,
                "audio",
            ),
            (
                &self.ingress_video,
                &self.output.video,
                VIDEO_REPAIR_WINDOW,
                "video",
            ),
        ];
        for (ingress, output, allowance, medium) in media {
            if let Some(stall) = medium_stall(ingress, output) {
                let counted = stall.saturating_sub(allowance);
                if counted > idle.0 {
                    idle = (counted, StallSide::MediumOutput(medium));
                }
            }
        }
        Some(idle)
    }

    /// Each medium of `mode` that is not reaching the flow while the publisher
    /// is still connected, with where it stops. For reporting only; nothing
    /// here feeds `idle`, so it never reaps or displaces a session.
    ///
    /// Arrival and output are read per medium, so the three cases come apart:
    /// - `PublisherStopped`: the medium arrived and then stopped arriving,
    ///   while the other medium still arrives. Requires the medium to have been
    ///   silent for its own budget *longer* than the other one, or a transport
    ///   that drops both at once reads as a lost microphone for the difference
    ///   between their budgets.
    /// - `NeverSent`: none of the medium has arrived, while the other one has
    ///   been arriving for `absent`. The chains start apart (`decodebin`
    ///   autoplugs each, video waits for a keyframe), hence the margin.
    /// - `NotProduced`: the medium still arrives but none of it comes out, as
    ///   measured by `medium_stall`. `idle` reaps this one in time.
    ///
    /// The first two need a counterpart, so only `StreamMode::AudioVideo` can
    /// report them; a single-medium seat that stops sending has stopped
    /// altogether, which is the watchdog's to judge.
    pub fn missing_media(&self, mode: StreamMode, budgets: MediumBudgets) -> Vec<MissingMedium> {
        let both = mode.has_audio() && mode.has_video();
        let mut missing = Vec::new();
        for medium in [HealthMedium::Audio, HealthMedium::Video] {
            let carried = match medium {
                HealthMedium::Audio => mode.has_audio(),
                HealthMedium::Video => mode.has_video(),
            };
            if !carried {
                continue;
            }
            let other = other_medium(medium);
            let ingress = self.ingress_of(medium);
            let other_ingress = self.ingress_of(other);

            if both {
                // The publisher still sending the other medium is what makes
                // this a missing medium rather than a publisher that left.
                if let Some(other_idle) = other_ingress
                    .since_last()
                    .filter(|idle| *idle < budgets.of(other))
                {
                    match ingress.since_last() {
                        Some(silent) if silent >= other_idle + budgets.of(medium) => {
                            missing.push(MissingMedium {
                                medium,
                                fault: MediumFault::PublisherStopped,
                                for_: silent,
                            });
                            continue;
                        }
                        None => {
                            if let Some(running) = other_ingress
                                .since_first()
                                .filter(|running| *running >= budgets.absent)
                            {
                                missing.push(MissingMedium {
                                    medium,
                                    fault: MediumFault::NeverSent,
                                    for_: running,
                                });
                            }
                            continue;
                        }
                        Some(_) => {}
                    }
                }
            }

            if let Some(stall) = medium_stall(ingress, self.output_of(medium))
                .filter(|stall| *stall >= budgets.of(medium))
            {
                missing.push(MissingMedium {
                    medium,
                    fault: MediumFault::NotProduced,
                    for_: stall,
                });
            }
        }
        missing
    }

    fn ingress_of(&self, medium: HealthMedium) -> &ActivityStamp {
        match medium {
            HealthMedium::Audio => &self.ingress_audio,
            HealthMedium::Video => &self.ingress_video,
        }
    }

    fn output_of(&self, medium: HealthMedium) -> &ActivityStamp {
        match medium {
            HealthMedium::Audio => &self.output.audio,
            HealthMedium::Video => &self.output.video,
        }
    }
}

fn other_medium(medium: HealthMedium) -> HealthMedium {
    match medium {
        HealthMedium::Audio => HealthMedium::Video,
        HealthMedium::Video => HealthMedium::Audio,
    }
}

/// How long a medium may be missing before `SessionActivity::missing_media`
/// reports it.
///
/// The media get different budgets because their cadences differ. A
/// connected publisher sends audio at least every 400 ms even through silence
/// (Opus DTX), so a couple of seconds of nothing is already a fault. Video has
/// no such floor: a screen share of a window nobody touches can go seconds
/// between frames. Video's budget is also `VIDEO_REPAIR_WINDOW`, so a frozen
/// decoder is not reported while keyframe repair still has a chance.
#[derive(Debug, Clone, Copy)]
pub struct MediumBudgets {
    pub audio: Duration,
    pub video: Duration,
    /// A medium that has never arrived, measured from the other medium's first
    /// arrival.
    pub absent: Duration,
}

impl Default for MediumBudgets {
    fn default() -> Self {
        Self {
            audio: Duration::from_secs(2),
            video: VIDEO_REPAIR_WINDOW,
            absent: Duration::from_secs(10),
        }
    }
}

impl MediumBudgets {
    fn of(&self, medium: HealthMedium) -> Duration {
        match medium {
            HealthMedium::Audio => self.audio,
            HealthMedium::Video => self.video,
        }
    }
}

/// One medium of a session that is not reaching the flow; see
/// `SessionActivity::missing_media`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingMedium {
    pub medium: HealthMedium,
    pub fault: MediumFault,
    /// How long it has been missing: since it stopped arriving
    /// (`PublisherStopped`), since the other medium started (`NeverSent`), or
    /// how long it has arrived with none of it coming out (`NotProduced`).
    pub for_: Duration,
}

/// Per-medium liveness of one WHIP Input block's seats, for the flow's block
/// health scan.
///
/// A seat medium that carries nothing does not stall anything: its appsrc is
/// simply never pushed to, every element downstream sits idle and `PLAYING`,
/// and the pad-task scan has nothing to find. The session's stamps are the
/// only evidence, so this reads them for each occupied slot.
///
/// It only reports. Whether a seat is reaped or displaced is
/// `SessionActivity::idle`'s call, and a publisher that stopped sending a
/// medium is deliberately not held against its seat there.
pub struct WhipSlotLiveness {
    endpoint_id: String,
    mode: StreamMode,
    slot_activity: Arc<Vec<Mutex<Weak<SessionActivity>>>>,
    /// An unoccupied slot is never reported, whatever its last session left.
    assignments: Arc<RwLock<Vec<Option<String>>>>,
}

/// One missing medium of one occupied slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotMediumStall {
    pub slot: usize,
    pub missing: MissingMedium,
}

impl std::fmt::Display for SlotMediumStall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let medium = self.missing.medium.as_str();
        let other = other_medium(self.missing.medium).as_str();
        let secs = self.missing.for_.as_secs_f32();
        match self.missing.fault {
            MediumFault::PublisherStopped => write!(
                f,
                "slot {}: the publisher stopped sending {} {:.1} s ago and still sends {}",
                self.slot, medium, secs, other
            ),
            MediumFault::NeverSent => write!(
                f,
                "slot {}: the publisher has sent no {} in {:.1} s of sending {}",
                self.slot, medium, secs, other
            ),
            MediumFault::NotProduced => write!(
                f,
                "slot {}: {} still arrives, but none has come out of the slot's chain for {:.1} s",
                self.slot, medium, secs
            ),
        }
    }
}

impl WhipSlotLiveness {
    pub fn new(
        endpoint_id: String,
        mode: StreamMode,
        slot_activity: Arc<Vec<Mutex<Weak<SessionActivity>>>>,
        assignments: Arc<RwLock<Vec<Option<String>>>>,
    ) -> Self {
        Self {
            endpoint_id,
            mode,
            slot_activity,
            assignments,
        }
    }

    /// Every missing medium of every occupied slot. `budgets` is a parameter
    /// so a test can drive the real decision without waiting out the
    /// production ones.
    pub fn stalls(&self, budgets: MediumBudgets) -> Vec<SlotMediumStall> {
        let occupied = self.assignments.read().unwrap();
        let mut stalls = Vec::new();
        for (slot, cell) in self.slot_activity.iter().enumerate() {
            if !occupied.get(slot).is_some_and(|holder| holder.is_some()) {
                continue;
            }
            let Some(activity) = cell.lock().unwrap().upgrade() else {
                continue;
            };
            stalls.extend(
                activity
                    .missing_media(self.mode, budgets)
                    .into_iter()
                    .map(|missing| SlotMediumStall { slot, missing }),
            );
        }
        stalls
    }
}

impl crate::blocks::BlockLiveness for WhipSlotLiveness {
    fn failure(&self) -> Option<crate::blocks::LivenessFailure> {
        let stalls = self.stalls(MediumBudgets::default());
        if stalls.is_empty() {
            return None;
        }
        Some(crate::blocks::LivenessFailure {
            detail: format!(
                "WHIP endpoint '{}': {}",
                self.endpoint_id,
                stalls
                    .iter()
                    .map(|stall| stall.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            causes: stalls
                .iter()
                .map(|stall| BlockHealthCause::WhipMedium {
                    slot: stall.slot as u32,
                    medium: stall.missing.medium,
                    fault: stall.missing.fault,
                })
                .collect(),
        })
    }
}

/// How long a session may receive media without any of it coming out of its
/// slot's chain before it counts as producing nothing.
///
/// Some delay is normal: H.264 cannot be decoded until a keyframe brings its
/// parameter sets, and `decodebin` has to autoplug a decoder first. Measured
/// from the session's *first* buffer, so a session that spent a minute
/// negotiating still gets the full grace once media starts. It has to stay under
/// the watchdog's `INACTIVITY_TIMEOUT` for the watchdog to reap a session that
/// never decodes at all, which `whip/watchdog.rs` asserts at compile time.
pub(crate) const DECODE_GRACE: Duration = Duration::from_secs(5);

/// How much staler `output` must be than `ingress` before a reap is blamed on
/// the flow rather than on the publisher; see `StallSide`. It moves the label
/// only, never the idle duration.
///
/// The two stamps keep separate epochs and store whole milliseconds, so
/// simultaneous events can read a millisecond apart either way, and a publisher
/// going away freezes both within one buffer of each other. The fault this
/// margin exists to name holds `ingress` live for seconds.
const STALL_SIDE_MARGIN: Duration = Duration::from_millis(100);

#[cfg(test)]
mod tests {
    use super::super::takeover::TAKEOVER_IDLE_THRESHOLD;
    use super::super::test_support::*;
    use super::*;
    use std::sync::atomic::AtomicBool;

    /// The output stamp belongs to the slot, which outlives the sessions passing
    /// through it. A new session must not be judged on frames its predecessor
    /// produced: inheriting a stale one makes the newcomer look stalled the
    /// moment its own media starts arriving, and the next client evicts it.
    #[test]
    fn a_new_session_does_not_inherit_the_slots_previous_liveness() {
        let slot_output = out(ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO));
        assert!(
            slot_output.last() != 0,
            "the previous occupant left the slot's stamp set"
        );

        let session = SessionActivity::new(Instant::now(), slot_output.clone());
        session.touch_ingress(true);

        assert_eq!(
            slot_output.last(),
            0,
            "claiming a slot must clear what the previous session left on it"
        );
        assert_eq!(
            session.idle(),
            None,
            "a session whose own media has only just started must not be judged yet"
        );
    }

    /// After a takeover, frames the displaced session had already pushed into
    /// the slot's chain can cross the tee after the newcomer reset the stamp.
    /// They must not stand in for the newcomer's own first decoded frame: they
    /// stop within moments, and judging on them would let the next client evict
    /// a session whose decoder has not had its keyframe yet.
    #[test]
    fn a_predecessors_trailing_frames_do_not_cut_the_decode_grace_short() {
        let slot_output = out(ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO));
        let session = SessionActivity::from_stamps(
            ActivityStamp::backdated(Duration::from_millis(200), Duration::ZERO),
            slot_output.clone(),
        );
        slot_output.reset();
        // The predecessor's tail crosses the tee, then the newcomer's media arrives.
        slot_output.audio.touch();
        std::thread::sleep(Duration::from_millis(50));
        session.touch_ingress(true);

        assert_eq!(
            session.idle(),
            None,
            "a session inside its decode grace must not be judged on its predecessor's frames"
        );
    }

    /// Both faults reap the seat, but they have different suspects, and the reap
    /// log is the only place an operator sees which one it was: a participant
    /// who went away, or a consumer inside their own flow that is blocking the
    /// slot's chain and will do the same to whoever reconnects.
    #[test]
    fn a_reaped_session_names_which_side_of_the_chain_stopped() {
        // Arrivals stopped first and the buffers already in the slot's chain
        // drained out after them, which is the order a publisher going away
        // always produces.
        let publisher_gone = SessionActivity::from_stamps(
            ActivityStamp::backdated(RUNNING_FOR, RUNNING_FOR / 2),
            out(ActivityStamp::backdated(
                RUNNING_FOR,
                RUNNING_FOR / 2 - Duration::from_secs(1),
            )),
        );
        assert_eq!(
            publisher_gone.idle().map(|(_, side)| side),
            Some(StallSide::Ingress),
            "nothing arriving is the publisher's fault, not the flow's"
        );

        // Still receiving; the slot's chain stopped producing a while ago.
        let stuck_consumer = SessionActivity::from_stamps(
            ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO),
            out(ActivityStamp::backdated(RUNNING_FOR, RUNNING_FOR / 2)),
        );
        assert_eq!(
            stuck_consumer.idle().map(|(_, side)| side),
            Some(StallSide::Output),
            "a seat that still receives must be reaped against the slot's output"
        );

        // The stamps hold whole milliseconds against separate epochs, so a
        // publisher drop that freezes both at once can read either way round.
        // Anything inside the margin must not be reported as a stuck consumer,
        // and the label must not change how long the session counts as idle.
        let near_tie_output = out(ActivityStamp::backdated(
            RUNNING_FOR,
            RUNNING_FOR / 2 + STALL_SIDE_MARGIN / 2,
        ));
        let near_tie = SessionActivity::from_stamps(
            ActivityStamp::backdated(RUNNING_FOR, RUNNING_FOR / 2),
            near_tie_output.clone(),
        );
        let output_idle = near_tie_output.since_last().unwrap();
        let (idle, side) = near_tie.idle().unwrap();
        assert_eq!(
            side,
            StallSide::Ingress,
            "stamp noise inside the margin must not move the blame to the flow"
        );
        assert!(
            idle >= output_idle,
            "idle must be the staler stamp whichever side is named: {idle:?} < {output_idle:?}"
        );
    }

    /// The watchdog reads `idle` too, so it reaps the same seat, and names the
    /// flow rather than the publisher. The repair window comes off the top.
    #[test]
    fn frozen_video_counts_as_idle_once_repair_has_had_its_chance() {
        let frozen_for = VIDEO_REPAIR_WINDOW + Duration::from_secs(20);
        let stop = Arc::new(AtomicBool::new(false));
        let activity = audio_and_video(stop.clone(), Duration::ZERO, Duration::ZERO, frozen_for);
        let (idle, side) = activity.idle().expect("past the decode grace");
        stop.store(true, Ordering::SeqCst);

        assert_eq!(
            side,
            StallSide::MediumOutput("video"),
            "the publisher is still sending, and it is the video that stalled"
        );
        assert!(
            idle >= Duration::from_secs(19) && idle <= Duration::from_secs(21),
            "idle must be the freeze less the repair window: {idle:?}"
        );
    }

    /// A camera turned back on after a long pause, or a mic unmuted: the
    /// medium resumes while the last buffer of it out of the slot is as old as
    /// the pause. The pause was the publisher's, so it must not count as a
    /// stall, not even for the moment before the decoder's first new frame.
    #[test]
    fn a_medium_that_resumes_after_a_pause_is_not_stalled() {
        let paused = VIDEO_REPAIR_WINDOW + Duration::from_secs(30);
        for medium in ["audio", "video"] {
            let ingress = ActivityStamp::backdated(RUNNING_FOR, paused);
            ingress.touch();
            let output = ActivityStamp::backdated(RUNNING_FOR, paused);
            assert_eq!(
                medium_stall(&ingress, &output).map(|stall| stall < ARRIVAL_PAUSE),
                Some(true),
                "{medium} that just resumed after {paused:?} is not stalled"
            );
        }
    }

    /// A medium that keeps arriving without a break is judged from the last
    /// buffer out, however long ago that was.
    #[test]
    fn a_medium_arriving_without_a_break_is_judged_from_its_last_output() {
        let ingress = ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO);
        ingress.touch();
        let output = ActivityStamp::backdated(RUNNING_FOR, Duration::from_secs(20));
        let stall = medium_stall(&ingress, &output).expect("past the grace");
        assert!(
            stall >= Duration::from_secs(19) && stall <= Duration::from_secs(21),
            "{stall:?}"
        );
    }

    /// Output stamped long before this medium first arrived (the previous
    /// occupant's trailing frames, or the other half of a session that sent
    /// audio first) is not this medium's stall. It counts from the end of the
    /// medium's own decode grace at the most.
    #[test]
    fn output_older_than_the_mediums_grace_is_not_held_against_it() {
        let started = DECODE_GRACE + Duration::from_secs(1);
        let ingress = ActivityStamp::backdated(started, Duration::ZERO);
        let output = ActivityStamp::backdated(RUNNING_FOR, RUNNING_FOR / 2);
        let stall = medium_stall(&ingress, &output).expect("past the grace");
        assert!(
            stall <= Duration::from_millis(1100),
            "a medium {started:?} old cannot have stalled for {stall:?}"
        );
    }

    /// The window has to outlast keyframe repair, or a seat is judged dead
    /// while the session is still asking its publisher for the keyframe that
    /// would revive it.
    #[test]
    fn the_video_repair_window_outlasts_keyframe_repair() {
        let policy = crate::gst::keyframe_request::RecoveryPolicy::default();
        assert!(VIDEO_REPAIR_WINDOW >= policy.interval * policy.attempts);
    }

    /// A seat positioned in time per medium: how long ago each medium last
    /// arrived and last came out (`None`: never), after `RUNNING_FOR` of
    /// session.
    fn seat(
        audio_in: Option<Duration>,
        audio_out: Option<Duration>,
        video_in: Option<Duration>,
        video_out: Option<Duration>,
    ) -> SessionActivity {
        let stamp = |idle: Option<Duration>| match idle {
            Some(idle) => ActivityStamp::backdated(RUNNING_FOR, idle),
            None => ActivityStamp::new(Instant::now()),
        };
        let newest = [audio_in, video_in].into_iter().flatten().min();
        SessionActivity::from_media_stamps(
            stamp(newest),
            stamp(audio_in),
            stamp(video_in),
            Arc::new(SlotOutput {
                audio: Arc::new(stamp(audio_out)),
                video: Arc::new(stamp(video_out)),
            }),
        )
    }

    fn faults(activity: &SessionActivity) -> Vec<(HealthMedium, MediumFault)> {
        activity
            .missing_media(StreamMode::AudioVideo, MediumBudgets::default())
            .into_iter()
            .map(|missing| (missing.medium, missing.fault))
            .collect()
    }

    const NOW: Option<Duration> = Some(Duration::ZERO);

    /// The 2026-10-06 show: a guest's browser stopped sending audio (its
    /// microphone track ended) while its camera kept sending. `idle` rightly
    /// does not hold that against the seat, so this report is the only
    /// signal an operator gets.
    #[test]
    fn a_publisher_that_stopped_sending_audio_is_reported_as_such() {
        let silent = Some(Duration::from_secs(6));
        let activity = seat(silent, silent, NOW, NOW);

        assert_eq!(
            faults(&activity),
            vec![(HealthMedium::Audio, MediumFault::PublisherStopped)]
        );
        assert!(
            activity
                .idle()
                .is_some_and(|(idle, _)| idle < TAKEOVER_IDLE_THRESHOLD),
            "the seat is not idle to the reaper, so it is not displaced: {:?}",
            activity.idle()
        );
    }

    /// The same seat with the video gone instead: a camera unplugged while the
    /// microphone carries on. Video's budget is longer, so it is reported
    /// later.
    #[test]
    fn a_publisher_that_stopped_sending_video_is_reported_after_its_budget() {
        let early = Some(Duration::from_secs(5));
        assert!(faults(&seat(NOW, NOW, early, early)).is_empty());

        let late = Some(VIDEO_REPAIR_WINDOW + Duration::from_secs(1));
        assert_eq!(
            faults(&seat(NOW, NOW, late, late)),
            vec![(HealthMedium::Video, MediumFault::PublisherStopped)]
        );
    }

    /// A transport that goes away stops both media within a frame of each
    /// other. Audio is past its 2 s budget long before video reaches its 10 s,
    /// and that window must not read as a lost microphone: the seat is gone,
    /// and the watchdog owns it.
    #[test]
    fn a_seat_that_lost_its_transport_is_not_reported() {
        for audio_idle in [3, 6, 9, 12, 20] {
            let audio = Some(Duration::from_secs(audio_idle));
            let video = Some(Duration::from_secs(audio_idle) - Duration::from_millis(30));
            assert!(
                faults(&seat(audio, audio, video, video)).is_empty(),
                "both media stopped together {audio_idle} s ago"
            );
        }
    }

    /// A publisher that sends video only on an `audio_video` endpoint: to an
    /// operator, the same silent guest as one whose microphone died.
    #[test]
    fn a_publisher_that_never_sent_audio_is_reported_once_video_has_run_a_while() {
        let activity = seat(None, None, NOW, NOW);
        assert_eq!(
            faults(&activity),
            vec![(HealthMedium::Audio, MediumFault::NeverSent)]
        );

        // Video that started a moment ago: audio may simply not have arrived
        // yet.
        let starting = SessionActivity::from_media_stamps(
            ActivityStamp::backdated(Duration::from_secs(3), Duration::ZERO),
            ActivityStamp::new(Instant::now()),
            ActivityStamp::backdated(Duration::from_secs(3), Duration::ZERO),
            Arc::new(SlotOutput::new(Instant::now())),
        );
        assert!(faults(&starting).is_empty());
    }

    /// Audio that still arrives with none of it coming out is a fault inside
    /// the flow, not the publisher's. `idle` reaps it in time; the report
    /// says which it is until then.
    #[test]
    fn audio_arriving_with_none_coming_out_is_reported_as_not_produced() {
        let stuck = Some(Duration::from_secs(5));
        assert_eq!(
            faults(&seat(NOW, stuck, NOW, NOW)),
            vec![(HealthMedium::Audio, MediumFault::NotProduced)]
        );
    }

    /// A frozen picture is not reported while keyframe repair still has a
    /// chance, and is once it has given up.
    #[test]
    fn frozen_video_is_reported_only_past_the_repair_window() {
        let repairing = Some(VIDEO_REPAIR_WINDOW - Duration::from_secs(3));
        assert!(faults(&seat(NOW, NOW, NOW, repairing)).is_empty());

        let wedged = Some(VIDEO_REPAIR_WINDOW + Duration::from_secs(3));
        assert_eq!(
            faults(&seat(NOW, NOW, NOW, wedged)),
            vec![(HealthMedium::Video, MediumFault::NotProduced)]
        );
    }

    #[test]
    fn a_healthy_seat_or_a_single_medium_endpoint_reports_nothing() {
        assert!(faults(&seat(NOW, NOW, NOW, NOW)).is_empty());

        let video_only = seat(None, None, NOW, NOW);
        assert!(video_only
            .missing_media(StreamMode::Video, MediumBudgets::default())
            .is_empty());
    }

    /// The block's reporter reads whichever session last claimed each slot,
    /// and only while someone holds the slot.
    #[test]
    fn slot_liveness_reads_the_session_holding_each_slot() {
        let config = endpoint_config(2);
        let liveness = WhipSlotLiveness::new(
            "guests".to_string(),
            StreamMode::AudioVideo,
            config.slot_activity.clone(),
            config.slot_assignments.clone(),
        );
        let budgets = MediumBudgets {
            audio: Duration::from_millis(50),
            video: Duration::from_millis(50),
            absent: Duration::from_millis(50),
        };

        assert_eq!(config.allocate_slot("guest-a"), Some(0));
        assert_eq!(config.allocate_slot("guest-b"), Some(1));
        let a = config.start_session_activity(0);
        let b = config.start_session_activity(1);
        a.touch_ingress(false);
        b.touch_ingress(true);
        b.touch_ingress(false);
        std::thread::sleep(Duration::from_millis(80));
        b.touch_ingress(true);
        b.touch_ingress(false);
        a.touch_ingress(false);

        let stalls = liveness.stalls(budgets);
        assert_eq!(
            stalls
                .iter()
                .map(|stall| (stall.slot, stall.missing.medium, stall.missing.fault))
                .collect::<Vec<_>>(),
            vec![(0, HealthMedium::Audio, MediumFault::NeverSent)],
            "only slot 0's publisher sends no audio: {stalls:?}"
        );
        let failure = crate::blocks::BlockLiveness::failure(&liveness);
        assert!(failure.is_none(), "production budgets are not spent yet");

        drop(a);
        assert!(
            liveness.stalls(budgets).is_empty(),
            "a slot whose session is gone has nothing to report"
        );

        let a = config.start_session_activity(0);
        a.touch_ingress(false);
        std::thread::sleep(Duration::from_millis(80));
        a.touch_ingress(false);
        b.touch_ingress(true);
        b.touch_ingress(false);
        assert_eq!(
            liveness.stalls(budgets).len(),
            1,
            "slot 0's new session sends no audio either"
        );
        assert!(config.release_slot(0, "guest-a"));
        assert!(
            liveness.stalls(budgets).is_empty(),
            "a released slot is not reported, whatever its last session left"
        );
    }
}
