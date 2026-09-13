//! Tests against a real `cefsrc`, the gstcefsrc element behind HTML sources and
//! DSK graphics.
//!
//! `cefsrc` is not in the distro GStreamer packages, so these skip wherever it
//! is missing — which is every CI job except `Test (Linux, cefsrc)`. That job
//! sets `STROM_REQUIRE_CEF=1`, which turns the skip into a failure.
//!
//! Chromium paints only when the page changes, so an idle page emits no
//! buffers. Every page here animates something to keep frames coming.
//!
//! CEF initialises once per process and is slow the first time, so every wait
//! polls against a generous deadline, and the tests run serially.

use base64::Engine;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use strom::state::AppState;
use strom::storage::JsonFileStorage;
use strom_types::{Flow, FlowId, Link, PropertyValue};
use tempfile::NamedTempFile;

/// Budget for the first frame. CEF's first initialisation in a process takes
/// several seconds on a CI runner.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(60);

/// Returns whether `cefsrc` can run here. Panics instead of returning false when
/// `STROM_REQUIRE_CEF` is set, so the CI job cannot pass by skipping.
fn cefsrc_available() -> bool {
    gst::init().unwrap();
    let required = strom_types::env::var_opt("STROM_REQUIRE_CEF").is_some();

    // On macOS CEF needs a Cocoa run loop on the main thread, which the test
    // harness does not provide: starting a cefsrc blocks forever.
    if !cfg!(target_os = "linux") {
        assert!(
            !required,
            "STROM_REQUIRE_CEF is set, but these tests run on Linux only"
        );
        eprintln!("SKIP: cefsrc tests run on Linux only");
        return false;
    }

    if gst::ElementFactory::find("cefsrc").is_some() {
        return true;
    }
    assert!(
        !required,
        "STROM_REQUIRE_CEF is set but the cefsrc element is not installed \
         (check GST_PLUGIN_PATH)"
    );
    eprintln!("SKIP: cefsrc not installed (set STROM_REQUIRE_CEF=1 to fail instead)");
    false
}

/// A `data:` URL for a transparent page covered by `body_css`, with a small
/// corner square that changes every animation frame so Chromium keeps painting.
fn animated_page_url(body_css: &str) -> String {
    let html = format!(
        "<!doctype html><html><body style=\"margin:0;{body_css}\">\
         <div id=\"tick\" style=\"position:fixed;left:0;top:0;width:4px;height:4px\"></div>\
         <script>\
         let n = 0;\
         const tick = document.getElementById('tick');\
         (function frame() {{\
           tick.style.background = (n++ % 2) ? '#f00' : '#00f';\
           requestAnimationFrame(frame);\
         }})();\
         </script></body></html>"
    );
    format!(
        "data:text/html;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(html)
    )
}

fn element(id: &str, element_type: &str, props: &[(&str, PropertyValue)]) -> strom_types::Element {
    strom_types::Element {
        id: id.to_string(),
        element_type: element_type.to_string(),
        properties: props
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
        position: [0.0, 0.0].into(),
        pad_properties: HashMap::new(),
    }
}

fn link(from: &str, to: &str) -> Link {
    Link {
        from: from.to_string(),
        to: to.to_string(),
    }
}

fn string(s: &str) -> PropertyValue {
    PropertyValue::String(s.to_string())
}

/// `cefsrc -> appsink`, with the appsink's caps choosing the render size.
fn source_tap_flow(name: &str, url: &str) -> Flow {
    let mut flow = Flow::new(name);
    flow.elements
        .push(element("cef", "cefsrc", &[("url", string(url))]));
    flow.elements.push(element(
        "tap",
        "appsink",
        &[
            (
                "caps",
                string("video/x-raw,format=BGRA,width=320,height=180"),
            ),
            ("sync", PropertyValue::Bool(false)),
            ("max-buffers", PropertyValue::UInt(2)),
            ("drop", PropertyValue::Bool(true)),
        ],
    ));
    flow.links.push(link("cef:src", "tap:sink"));
    flow
}

fn new_state() -> (AppState, NamedTempFile, NamedTempFile) {
    let storage_file = NamedTempFile::new().unwrap();
    let blocks_file = NamedTempFile::new().unwrap();
    let state = AppState::new(
        JsonFileStorage::new(storage_file.path()),
        blocks_file.path(),
        std::env::temp_dir(),
        vec![],
        "all".to_string(),
        vec![],
    );
    (state, storage_file, blocks_file)
}

/// Looks up an element of a running flow. The caller must drop the returned
/// reference before stopping the flow, or the pipeline cannot finalize.
async fn running_element(state: &AppState, flow_id: &FlowId, name: &str) -> gst::Element {
    let pipelines = state.pipelines_read().await;
    pipelines
        .get(flow_id)
        .expect("flow is running")
        .pipeline()
        .by_name(name)
        .unwrap_or_else(|| panic!("element {name} in pipeline"))
}

/// Pulls samples until `accept` returns true for one, or the deadline passes.
fn pull_until(
    appsink: &gst_app::AppSink,
    timeout: Duration,
    mut accept: impl FnMut(&gst::Sample) -> bool,
) -> Option<gst::Sample> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(sample) = appsink.try_pull_sample(gst::ClockTime::from_mseconds(200)) {
            if accept(&sample) {
                return Some(sample);
            }
        }
    }
    None
}

/// BGRA bytes of the pixel at (x, y).
fn pixel(sample: &gst::Sample, x: usize, y: usize) -> [u8; 4] {
    let caps = sample.caps().expect("sample caps");
    let info = gstreamer_video::VideoInfo::from_caps(caps).expect("video caps");
    assert_eq!(info.format(), gstreamer_video::VideoFormat::Bgra);
    let buffer = sample.buffer().expect("sample buffer");
    let map = buffer.map_readable().expect("readable buffer");
    let offset = info.offset()[0] + y * info.stride()[0] as usize + x * 4;
    map[offset..offset + 4].try_into().unwrap()
}

/// Threads of this process, from `/proc/self/task`.
fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task").unwrap().count()
}

/// Every process descended from this one. CEF's browser, zygote, GPU and
/// renderer processes are children and grandchildren of the test process.
fn descendant_processes() -> Vec<(u32, String)> {
    let mut parent_of = HashMap::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        // The command name is parenthesised and may itself contain spaces or
        // parentheses, so split after the last ')'.
        let Some((head, tail)) = stat.rsplit_once(')') else {
            continue;
        };
        let comm = head
            .split_once('(')
            .map(|(_, c)| c)
            .unwrap_or("")
            .to_string();
        if let Some(ppid) = tail.split_whitespace().nth(1).and_then(|p| p.parse().ok()) {
            parent_of.insert(pid, (ppid, comm));
        }
    }
    let me = std::process::id();
    let mut out = Vec::new();
    for (&pid, (_, comm)) in &parent_of {
        let mut ancestor = pid;
        while let Some(&(ppid, _)) = parent_of.get(&ancestor) {
            if ppid == me {
                out.push((pid, comm.clone()));
                break;
            }
            ancestor = ppid;
        }
    }
    out.sort();
    out
}

/// A flow with a `cefsrc` starts, delivers frames, stops and is fully finalized,
/// across repeated restarts, without leaving threads or CEF processes behind.
///
/// The first cycle is the baseline: CEF initialises once per process and keeps
/// its browser-process threads for the life of the process. Later cycles must
/// return to that baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn cefsrc_flow_restarts_without_leaking() {
    if !cefsrc_available() {
        return;
    }

    const CYCLES: usize = 4;
    // Pooled GLib threads can outlive a cycle briefly, so a small excess over
    // the baseline is not a leak. One leaked thread per cycle exceeds it by the
    // last cycle.
    const THREAD_SLACK: usize = 2;

    let (state, _storage, _blocks) = new_state();
    let flow = source_tap_flow("cefsrc_lifecycle", &animated_page_url(""));
    let flow_id = flow.id;
    state.upsert_flow(flow).await.expect("upsert_flow");

    let mut baseline: Option<(usize, usize)> = None;
    for cycle in 1..=CYCLES {
        state.start_flow(&flow_id).await.expect("start_flow");

        let (pipeline_weak, element_weak_refs) = {
            let pipelines = state.pipelines_read().await;
            let manager = pipelines.get(&flow_id).expect("flow is running");
            (manager.pipeline_weak(), manager.element_weak_refs())
        };

        let tap = running_element(&state, &flow_id, "tap")
            .await
            .downcast::<gst_app::AppSink>()
            .expect("tap is an appsink");
        let timeout = if cycle == 1 {
            FIRST_FRAME_TIMEOUT
        } else {
            Duration::from_secs(20)
        };
        let mut frames = 0;
        let delivered = tokio::task::block_in_place(|| {
            pull_until(&tap, timeout, |_| {
                frames += 1;
                frames >= 5
            })
        });
        assert!(
            delivered.is_some(),
            "cycle {cycle}: cefsrc delivered {frames} frames within {timeout:?}, expected 5"
        );
        drop(delivered);
        drop(tap);

        state.stop_flow(&flow_id).await.expect("stop_flow");

        // CEF closes its browser asynchronously, so give finalization a moment
        // rather than asserting the instant stop returns.
        let deadline = Instant::now() + Duration::from_secs(10);
        while pipeline_weak.upgrade().is_some() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            pipeline_weak.upgrade().is_none(),
            "cycle {cycle}: pipeline still alive 10s after stop_flow"
        );
        let leaked: Vec<_> = element_weak_refs
            .iter()
            .filter_map(|(name, weak)| weak.upgrade().map(|_| name.clone()))
            .collect();
        assert!(
            leaked.is_empty(),
            "cycle {cycle}: elements still alive after stop_flow: {leaked:?}"
        );

        match baseline {
            None => {
                // Let the first browser's renderer exit before sampling.
                tokio::time::sleep(Duration::from_secs(3)).await;
                let processes = descendant_processes();
                baseline = Some((thread_count(), processes.len()));
                eprintln!(
                    "baseline after cycle 1: {} threads, processes {:?}",
                    thread_count(),
                    processes
                );
            }
            Some((base_threads, base_processes)) => {
                let deadline = Instant::now() + Duration::from_secs(20);
                loop {
                    let threads = thread_count();
                    let processes = descendant_processes();
                    if threads <= base_threads + THREAD_SLACK && processes.len() <= base_processes {
                        eprintln!(
                            "cycle {cycle}: {threads} threads, {} processes",
                            processes.len()
                        );
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "cycle {cycle}: resources did not return to the cycle-1 baseline \
                         within 20s: {threads} threads (baseline {base_threads}, slack \
                         {THREAD_SLACK}), processes {processes:?} (baseline {base_processes})"
                    );
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }
}

/// `cefsrc` on a vision mixer DSK input delivers buffers to the compositor,
/// through a Video Format block setting only a resolution — the way HTML
/// graphics are wired onto a DSK.
///
/// A DSK chain can stall silently: when negotiation fails, `cefsrc`'s task pauses
/// on not-negotiated and nothing reaches the mixer, with no error on the bus.
/// The known case is a fixed framerate in the chain. gstcefsrc from `c89c2ce`
/// on advertises `framerate=0/1` (variable), which no fixed framerate can meet,
/// and nothing in the chain converts framerate. The pinned gstcefsrc predates
/// that and accepts 1-60 fps, so with today's pin a fixed framerate still
/// negotiates and this test passes either way; it guards that case once the
/// pin moves past `c89c2ce`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn cefsrc_on_dsk_input_reaches_mixer() {
    if !cefsrc_available() {
        return;
    }
    // The vision mixer's converters ask for the detected GPU mode, which panics
    // if nothing has probed for it — `main` does this at startup.
    strom::gpu::detect_gpu_capabilities();

    const VM: &str = "vm";

    let mut flow = Flow::new("cefsrc_dsk");
    flow.elements.push(element(
        "cef",
        "cefsrc",
        &[("url", string(&animated_page_url("")))],
    ));
    flow.blocks.push(strom_types::BlockInstance {
        id: "fmt".to_string(),
        block_definition_id: "builtin.videoformat".to_string(),
        name: None,
        properties: HashMap::from([("resolution".to_string(), string("640x360"))]),
        position: strom_types::block::Position { x: 0.0, y: 0.0 },
        runtime_data: None,
        computed_external_pads: None,
    });
    flow.blocks.push(strom_types::BlockInstance {
        id: VM.to_string(),
        block_definition_id: "builtin.vision_mixer".to_string(),
        name: None,
        properties: HashMap::from([
            ("compositor_preference".to_string(), string("cpu")),
            ("num_inputs".to_string(), PropertyValue::UInt(2)),
            ("num_dsk_inputs".to_string(), string("1")),
            ("pgm_resolution".to_string(), string("640x360")),
            ("multiview_resolution".to_string(), string("640x360")),
        ]),
        position: strom_types::block::Position { x: 0.0, y: 0.0 },
        runtime_data: None,
        computed_external_pads: None,
    });
    flow.elements.push(element("pgm_sink", "fakesink", &[]));
    flow.elements.push(element("mv_sink", "fakesink", &[]));
    flow.links.push(link("cef:src", "fmt:video_in"));
    flow.links.push(link("fmt:video_out", "vm:dsk_in_0"));
    flow.links.push(link("vm:pgm_out", "pgm_sink:sink"));
    flow.links.push(link("vm:multiview_out", "mv_sink:sink"));

    let (state, _storage, _blocks) = new_state();
    let flow_id = flow.id;
    state.upsert_flow(flow).await.expect("upsert_flow");
    state.start_flow(&flow_id).await.expect("start_flow");

    // The compositor pad fed by the DSK 0 chain, found by its link rather than
    // by index: the pad number depends on the input count after clamping.
    let mixer = running_element(&state, &flow_id, &format!("{VM}:mixer")).await;
    let dsk_pad = mixer
        .sink_pads()
        .into_iter()
        .find(|pad| {
            pad.peer()
                .and_then(|peer| peer.parent_element())
                .is_some_and(|upstream| upstream.name().ends_with("_dsk_0"))
        })
        .unwrap_or_else(|| {
            let pads: Vec<_> = mixer
                .sink_pads()
                .iter()
                .map(|pad| {
                    let upstream = pad.peer().and_then(|p| p.parent_element());
                    format!("{} <- {:?}", pad.name(), upstream.map(|e| e.name()))
                })
                .collect();
            panic!("no mixer sink pad is linked to the DSK 0 chain: {pads:?}")
        });

    let buffers = Arc::new(AtomicU64::new(0));
    let probe = {
        let buffers = buffers.clone();
        dsk_pad
            .add_probe(gst::PadProbeType::BUFFER, move |_, _| {
                buffers.fetch_add(1, Ordering::Relaxed);
                gst::PadProbeReturn::Ok
            })
            .expect("probe on DSK pad")
    };

    let deadline = Instant::now() + FIRST_FRAME_TIMEOUT;
    while buffers.load(Ordering::Relaxed) < 10 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let delivered = buffers.load(Ordering::Relaxed);
    // The source-to-mixer chain's pad caps, for the failure message: a
    // NOT NEGOTIATED pad shows where the chain stalled.
    let chain_caps: Vec<String> = {
        let pipelines = state.pipelines_read().await;
        let mut caps: Vec<String> = pipelines
            .get(&flow_id)
            .expect("flow is running")
            .get_all_pad_caps()
            .into_iter()
            .filter(|(element, _)| {
                element == "cef" || element.starts_with("fmt:") || element.contains("dsk")
            })
            .flat_map(|(element, pads)| {
                pads.into_iter()
                    .map(move |(pad, _, caps)| format!("{element}:{pad} = {caps}"))
            })
            .collect();
        caps.push(format!(
            "{VM}:mixer:{} = {:?}",
            dsk_pad.name(),
            dsk_pad.current_caps()
        ));
        caps.sort();
        caps
    };

    dsk_pad.remove_probe(probe);
    drop(dsk_pad);
    drop(mixer);
    state.stop_flow(&flow_id).await.expect("stop_flow");

    assert!(
        delivered >= 10,
        "the mixer's DSK pad received {delivered} buffers within {FIRST_FRAME_TIMEOUT:?}, \
         expected 10. Pad caps:\n  {}",
        chain_caps.join("\n  ")
    );
}

/// `cefsrc` emits premultiplied alpha: CSS `rgba(255,255,255,0.5)` over a
/// transparent page paints as BGRA 128,128,128,128, not straight 255,255,255,128.
///
/// Compositing HTML graphics correctly depends on knowing which alpha form they
/// arrive in, and caps cannot say. If a CEF or gstcefsrc bump changes it,
/// translucent graphics composite wrong with no error, so it is pinned here at
/// the source's output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn cefsrc_emits_premultiplied_alpha() {
    if !cefsrc_available() {
        return;
    }

    let (state, _storage, _blocks) = new_state();
    let flow = source_tap_flow(
        "cefsrc_alpha",
        &animated_page_url("background:rgba(255,255,255,0.5)"),
    );
    let flow_id = flow.id;
    state.upsert_flow(flow).await.expect("upsert_flow");
    state.start_flow(&flow_id).await.expect("start_flow");

    let tap = running_element(&state, &flow_id, "tap")
        .await
        .downcast::<gst_app::AppSink>()
        .expect("tap is an appsink");

    // Frames before the page loads can be transparent or opaque black, so a
    // non-zero centre alpha does not prove the page painted. The corner square
    // turning red or blue does: it is drawn by the page's own script. Sample
    // the centre, far from that square, once it has shown a few times.
    let mut painted = 0;
    let sample = tokio::task::block_in_place(|| {
        pull_until(&tap, FIRST_FRAME_TIMEOUT, |sample| {
            let [b, g, r, a] = pixel(sample, 1, 1);
            let red_or_blue = g < 32 && ((r > 223 && b < 32) || (b > 223 && r < 32));
            if a == 255 && red_or_blue {
                painted += 1;
            }
            painted >= 3
        })
    });
    let centre = sample.as_ref().map(|s| pixel(s, 160, 90));

    drop(sample);
    drop(tap);
    state.stop_flow(&flow_id).await.expect("stop_flow");

    let [b, g, r, a] = centre.expect("cefsrc never painted the page");
    let near = |v: u8, want: u8| v.abs_diff(want) <= 2;
    assert!(
        near(a, 128),
        "alpha of a 50% CSS fill is {a}, expected ~128 (BGRA {b},{g},{r},{a})"
    );
    assert!(
        near(b, 128) && near(g, 128) && near(r, 128),
        "50% white paints as BGRA {b},{g},{r},{a}: expected premultiplied ~128,128,128,128. \
         Straight alpha would read 255,255,255,128; if cefsrc changed to that, anything \
         treating its output as premultiplied over-brightens translucent HTML graphics"
    );
}

/// A page that covers the frame for one second after every `hashchange`, with a
/// hole in the bottom-right corner showing the program and a blue marker in the
/// top-left while it animates. Idle, it is fully transparent and paints nothing.
fn stinger_page_url() -> String {
    let html = "<!doctype html><html><body style=\"margin:0;background:transparent;overflow:hidden\">\
        <canvas id=\"c\" width=\"640\" height=\"360\"></canvas>\
        <script>\
        const x = document.getElementById('c').getContext('2d');\
        let t0 = 0;\
        function draw() {\
          x.clearRect(0, 0, 640, 360);\
          if (performance.now() - t0 >= 1000) return;\
          x.fillStyle = 'rgb(255,255,0)'; x.fillRect(0, 0, 640, 360);\
          x.clearRect(600, 320, 40, 40);\
          x.fillStyle = 'rgb(0,0,255)'; x.fillRect(0, 0, 80, 45);\
          requestAnimationFrame(draw);\
        }\
        addEventListener('hashchange', () => { t0 = performance.now(); requestAnimationFrame(draw); });\
        </script></body></html>";
    format!(
        "data:text/html;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(html)
    )
}

/// RGBA bytes of the pixel at (x, y) of an RGBA sample.
fn rgba_pixel(sample: &gst::Sample, x: usize, y: usize) -> [u8; 4] {
    let caps = sample.caps().expect("sample caps");
    let info = gstreamer_video::VideoInfo::from_caps(caps).expect("video caps");
    assert_eq!(info.format(), gstreamer_video::VideoFormat::Rgba);
    let buffer = sample.buffer().expect("sample buffer");
    let map = buffer.map_readable().expect("readable buffer");
    let offset = info.offset()[0] + y * info.stride()[0] as usize + x * 4;
    map[offset..offset + 4].try_into().unwrap()
}

/// An HTML graphic stinger cuts the program on the output frame that carries
/// its cut point, counted from the page's first frame on air.
///
/// The cut is anchored to the first frame the page paints after the take; on
/// wall clock it lands a frame or two early with a live page on the mixer. At
/// 30 fps a 500 ms cut point is exactly 15 frames, whatever the page's phase
/// against the mixer's frame grid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn html_graphic_stinger_cuts_on_the_frame_its_cut_point_names() {
    if !cefsrc_available() {
        return;
    }
    strom::gpu::detect_gpu_capabilities();

    const TAKES: usize = 6;
    const CUT_MS: u64 = 500;
    const EXPECTED_FRAMES: usize = 15;

    fn block(
        id: &str,
        definition: &str,
        props: &[(&str, PropertyValue)],
    ) -> strom_types::BlockInstance {
        strom_types::BlockInstance {
            id: id.to_string(),
            block_definition_id: definition.to_string(),
            name: None,
            properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            position: strom_types::block::Position { x: 0.0, y: 0.0 },
            runtime_data: None,
            computed_external_pads: None,
        }
    }

    let mut flow = Flow::new("html_stinger_cut");
    for (id, colour) in [("red", "0xffff0000"), ("green", "0xff00ff00")] {
        flow.elements.push(element(
            id,
            "videotestsrc",
            &[
                ("pattern", string("solid-color")),
                ("foreground-color", string(colour)),
                ("is-live", PropertyValue::Bool(true)),
            ],
        ));
        flow.elements.push(element(
            &format!("{id}_caps"),
            "capsfilter",
            &[(
                "caps",
                string("video/x-raw,width=640,height=360,framerate=30/1"),
            )],
        ));
        flow.links
            .push(link(&format!("{id}:src"), &format!("{id}_caps:sink")));
    }
    flow.blocks.push(block(
        "web",
        "builtin.html_graphic",
        &[
            ("url", string(&stinger_page_url())),
            ("resolution", string("640x360")),
            ("framerate", string("30/1")),
            ("stinger_source", PropertyValue::Bool(true)),
            ("stinger_duration_ms", PropertyValue::UInt(1000)),
            ("stinger_cut_point_ms", PropertyValue::UInt(CUT_MS)),
            ("stinger_under_transition", string("cut")),
        ],
    ));
    flow.blocks.push(block(
        "vm",
        "builtin.vision_mixer",
        &[
            ("compositor_preference", string("cpu")),
            ("num_inputs", PropertyValue::UInt(2)),
            ("num_dsk_inputs", PropertyValue::UInt(1)),
            ("dsk_0_alpha_mode", string("premultiplied")),
            ("pgm_resolution", string("640x360")),
            ("pgm_framerate", string("30")),
            ("multiview_resolution", string("640x360")),
        ],
    ));
    flow.elements
        .push(element("pgm_convert", "videoconvert", &[]));
    flow.elements.push(element(
        "pgm_tap",
        "appsink",
        &[
            ("caps", string("video/x-raw,format=RGBA")),
            ("sync", PropertyValue::Bool(false)),
            // Every frame, in order: dropping would renumber what is counted.
            ("max-buffers", PropertyValue::UInt(400)),
            ("drop", PropertyValue::Bool(false)),
        ],
    ));
    flow.elements.push(element("mv_sink", "fakesink", &[]));
    flow.links.push(link("red_caps:src", "vm:video_in_0"));
    flow.links.push(link("green_caps:src", "vm:video_in_1"));
    flow.links.push(link("web:video_out", "vm:dsk_in_0"));
    flow.links.push(link("vm:pgm_out", "pgm_convert:sink"));
    flow.links.push(link("pgm_convert:src", "pgm_tap:sink"));
    flow.links.push(link("vm:multiview_out", "mv_sink:sink"));

    let (state, _storage, _blocks) = new_state();
    let flow_id = flow.id;
    state.upsert_flow(flow).await.expect("upsert_flow");
    state.start_flow(&flow_id).await.expect("start_flow");

    let tap = running_element(&state, &flow_id, "pgm_tap")
        .await
        .downcast::<gst_app::AppSink>()
        .expect("pgm_tap is an appsink");

    // Let CEF initialise and load the page before the first take.
    tokio::task::block_in_place(|| {
        pull_until(&tap, FIRST_FRAME_TIMEOUT, |_| false);
    });

    let marker = |s: &gst::Sample| rgba_pixel(s, 20, 11)[..3] == [0, 0, 255];
    let program = |s: &gst::Sample| {
        let [r, g, b, _] = rgba_pixel(s, 630, 350);
        match (r, g, b) {
            (255, 0, 0) => Some(0),
            (0, 255, 0) => Some(1),
            _ => None,
        }
    };

    let mut landed = Vec::new();
    for take in 0..TAKES {
        while tap
            .try_pull_sample(gst::ClockTime::from_mseconds(1))
            .is_some()
        {}
        let mut events = state.events().subscribe();
        let (from, to) = if take % 2 == 0 { (0, 1) } else { (1, 0) };
        state
            .trigger_stinger(&flow_id, "vm", from, to, Some("web"))
            .await
            .expect("stinger must start");

        let counted = tokio::task::block_in_place(|| {
            let mut index = 0usize;
            let mut first_marker = None;
            let mut was = None;
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                let Some(sample) = tap.try_pull_sample(gst::ClockTime::from_mseconds(500)) else {
                    continue;
                };
                if first_marker.is_none() && marker(&sample) {
                    first_marker = Some(index);
                }
                if let Some(now) = program(&sample) {
                    if was.is_some_and(|w| w != now) {
                        return first_marker.map(|m| index - m);
                    }
                    was = Some(now);
                }
                index += 1;
            }
            None
        });
        landed.push(counted);

        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            match tokio::time::timeout_at(deadline.into(), events.recv()).await {
                Ok(Ok(strom_types::StromEvent::StingerCompleted { .. })) => break,
                Ok(Ok(_)) => continue,
                other => panic!("the stinger never completed: {other:?}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    drop(tap);
    state.stop_flow(&flow_id).await.expect("stop_flow");

    assert!(
        landed.iter().all(|l| *l == Some(EXPECTED_FRAMES)),
        "every take must change the program {EXPECTED_FRAMES} frames after the page \
         appears; frames counted per take: {landed:?}"
    );
}
