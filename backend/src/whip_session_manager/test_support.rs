//! Fixtures shared by the session manager's tests.

use super::*;

pub(super) fn dummy_session() -> (gst::Element, gst::Pipeline, Arc<AtomicBool>) {
    let _ = gst::init();
    let element = gst::ElementFactory::make("fakesrc")
        .build()
        .expect("fakesrc is part of gstreamer core");
    let pipeline = gst::Pipeline::new();
    (element, pipeline, Arc::new(AtomicBool::new(false)))
}

/// A slot output whose audio stamp is `audio`; its video has produced
/// nothing.
pub(super) fn out(audio: ActivityStamp) -> Arc<SlotOutput> {
    Arc::new(SlotOutput::with_audio(audio))
}

/// How long a fixture session has been running before the test looks at it.
/// Comfortably past `DECODE_GRACE`, so a session that has produced nothing
/// usable in that time is genuinely broken rather than still starting up.
pub(super) const RUNNING_FOR: Duration = Duration::from_secs(60);

/// Tick `stamp` every 20 ms until `stop` is set — the rate the session
/// appsink and the slot's output probe stamp a running publisher at.
pub(super) fn tick_until_stopped(stop: Arc<AtomicBool>, stamp: impl Fn() + Send + 'static) {
    std::thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            stamp();
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

/// A publisher whose transport is gone: nothing has arrived and nothing has
/// come out of its slot for `idle`. `idle` of zero is the case that matters
/// — a client reconnecting the instant its publisher died looks, at that
/// moment, exactly like a healthy one.
pub(super) fn dead_publisher(idle: Duration) -> Arc<SessionActivity> {
    let ran_for = RUNNING_FOR + idle;
    Arc::new(SessionActivity::from_stamps(
        ActivityStamp::backdated(ran_for, idle),
        out(ActivityStamp::backdated(ran_for, idle)),
    ))
}

/// A publisher that is still sending and whose media still comes out of its
/// slot: both stamps tick, the way the appsink callback and the tee probe do
/// for a running session. The caller sets `stop` to end it.
pub(super) fn live_publisher(stop: Arc<AtomicBool>) -> Arc<SessionActivity> {
    let output = out(ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO));
    let activity = Arc::new(SessionActivity::from_stamps(
        ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO),
        output.clone(),
    ));
    let ingress = activity.clone();
    tick_until_stopped(stop.clone(), move || ingress.touch_ingress(true));
    tick_until_stopped(stop, move || output.audio.touch());
    activity
}

/// A healthy video-only publisher of static content, mid-gap: its last frame
/// arrived `gap` ago, came out of its slot, and the next one is not due yet.
/// Never carried audio, so nothing about it can be judged on a two-second
/// silence.
pub(super) fn sparse_video_publisher(gap: Duration) -> Arc<SessionActivity> {
    let ran_for = RUNNING_FOR + gap;
    Arc::new(SessionActivity::video_only_from_stamps(
        ActivityStamp::backdated(ran_for, gap),
        out(ActivityStamp::backdated(ran_for, gap)),
    ))
}

/// A seat that has received RTP for a minute while nothing has come out of
/// its slot's chain — the decoder never got a usable
/// keyframe, or a consumer below the slot's tee is blocking it. Judged on
/// arriving bytes alone this seat looks perfectly healthy.
pub(super) fn receiving_but_never_usable(stop: Arc<AtomicBool>) -> Arc<SessionActivity> {
    let activity = Arc::new(SessionActivity::from_stamps(
        ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO),
        Arc::new(SlotOutput::new(Instant::now())),
    ));
    let ingress = activity.clone();
    tick_until_stopped(stop, move || ingress.touch_ingress(true));
    activity
}

/// A seat that decoded, then froze `stalled_for` ago, while RTP keeps
/// arriving.
pub(super) fn receiving_but_stalled(
    stop: Arc<AtomicBool>,
    stalled_for: Duration,
) -> Arc<SessionActivity> {
    let activity = Arc::new(SessionActivity::from_stamps(
        ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO),
        out(ActivityStamp::backdated(RUNNING_FOR, stalled_for)),
    ));
    let ingress = activity.clone();
    tick_until_stopped(stop, move || ingress.touch_ingress(true));
    activity
}

/// Media has only just started arriving and nothing has decoded yet. Normal:
/// H.264 cannot be decoded until a keyframe brings its parameter sets.
pub(super) fn still_prerolling(stop: Arc<AtomicBool>) -> Arc<SessionActivity> {
    let activity = Arc::new(SessionActivity::from_stamps(
        ActivityStamp::backdated(Duration::from_millis(200), Duration::ZERO),
        Arc::new(SlotOutput::new(Instant::now())),
    ));
    let ingress = activity.clone();
    tick_until_stopped(stop, move || ingress.touch_ingress(true));
    activity
}

/// A session that is still negotiating: connected, but nothing has arrived.
pub(super) fn no_media_yet() -> Arc<SessionActivity> {
    Arc::new(SessionActivity::new(
        Instant::now(),
        Arc::new(SlotOutput::new(Instant::now())),
    ))
}

/// A seat sending both media for a minute, both still arriving. Audio keeps
/// coming out of the slot; `audio_out_idle` and `video_out_idle` say how
/// long ago each medium last did, and `video_in_idle` how long ago its
/// video last arrived (zero: still arriving). The caller sets `stop` to end
/// the arrivals and the audio output.
pub(super) fn audio_and_video(
    stop: Arc<AtomicBool>,
    audio_out_idle: Duration,
    video_in_idle: Duration,
    video_out_idle: Duration,
) -> Arc<SessionActivity> {
    let output = Arc::new(SlotOutput {
        audio: Arc::new(ActivityStamp::backdated(RUNNING_FOR, audio_out_idle)),
        video: Arc::new(ActivityStamp::backdated(RUNNING_FOR, video_out_idle)),
    });
    let activity = Arc::new(SessionActivity::from_media_stamps(
        ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO),
        ActivityStamp::backdated(RUNNING_FOR, Duration::ZERO),
        ActivityStamp::backdated(RUNNING_FOR, video_in_idle),
        output.clone(),
    ));
    let publisher = activity.clone();
    let video_arriving = video_in_idle.is_zero();
    tick_until_stopped(stop.clone(), move || {
        publisher.touch_ingress(true);
        if video_arriving {
            publisher.touch_ingress(false);
        }
    });
    if audio_out_idle.is_zero() {
        tick_until_stopped(stop, move || output.audio.touch());
    }
    activity
}

pub(super) fn endpoint_config(max_sessions: usize) -> WhipEndpointConfig {
    WhipEndpointConfig::for_tests("endpoint", max_sessions)
}

/// A manager with one single-slot endpoint whose only slot is held by a
/// registered session with the given liveness — i.e. a full endpoint.
pub(super) fn full_endpoint(
    resource_id: &str,
    port: u16,
    activity: Arc<SessionActivity>,
) -> (
    Arc<WhipSessionManager>,
    Arc<WhipEndpointConfig>,
    Arc<AtomicBool>,
) {
    let manager = Arc::new(WhipSessionManager::new());
    manager.start_cleanup_task();
    manager.register_endpoint("endpoint".to_string(), endpoint_config(1));
    let config = manager
        .get_endpoint_config("endpoint")
        .expect("endpoint was just registered");

    assert_eq!(
        config.allocate_slot(resource_id),
        Some(0),
        "the endpoint starts with its only slot free"
    );
    let cleanup_sent = register_with_activity(&manager, resource_id, port, activity);
    assert_eq!(
        config.allocate_slot("someone-else"),
        None,
        "the endpoint is now full"
    );
    (manager, config, cleanup_sent)
}

/// A session's `cleanup_sent` flag is the only way to stop its inactivity
/// watchdog thread. Every path that removes a session must set it, or the
/// watchdog outlives the session and asks for cleanup of a port that is gone.
pub(super) fn register(
    manager: &WhipSessionManager,
    resource_id: &str,
    port: u16,
) -> Arc<AtomicBool> {
    register_with_activity(manager, resource_id, port, dead_publisher(Duration::ZERO))
}

fn register_with_activity(
    manager: &WhipSessionManager,
    resource_id: &str,
    port: u16,
    activity: Arc<SessionActivity>,
) -> Arc<AtomicBool> {
    register_in_slot(manager, resource_id, port, 0, activity)
}

/// The config registered for "endpoint" on `manager`, registering a
/// single-slot one first if there is none.
pub(super) fn registered_endpoint(manager: &WhipSessionManager) -> Arc<WhipEndpointConfig> {
    if let Some(config) = manager.get_endpoint_config("endpoint") {
        return config;
    }
    manager.register_endpoint("endpoint".to_string(), endpoint_config(1));
    manager
        .get_endpoint_config("endpoint")
        .expect("endpoint was just registered")
}

pub(super) fn register_in_slot(
    manager: &WhipSessionManager,
    resource_id: &str,
    port: u16,
    slot: usize,
    activity: Arc<SessionActivity>,
) -> Arc<AtomicBool> {
    let (element, pipeline, cleanup_sent) = dummy_session();
    let registered = manager.register_session(NewWhipSession {
        resource_id: resource_id.to_string(),
        port,
        element,
        session_pipeline: pipeline,
        endpoint_id: "endpoint".to_string(),
        slot,
        config: registered_endpoint(manager),
        cleanup_sent: cleanup_sent.clone(),
        activity,
    });
    assert!(registered, "session should register");
    assert!(
        !cleanup_sent.load(Ordering::SeqCst),
        "a freshly registered session is not finished"
    );
    cleanup_sent
}
