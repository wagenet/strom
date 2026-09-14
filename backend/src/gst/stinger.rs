//! Stinger transitions — binding resolution and request validation.
//!
//! A stinger plays a keyed overlay over the program while another transition
//! runs beneath it. The overlay is a clip from a media player, or a page from an
//! HTML graphic, wired into one of the mixer's keyed (DSK) inputs; this module
//! works out *which* input, and rejects
//! the requests that cannot be honoured, before anything touches the pipeline.
//!
//! Everything here is pure — it reasons over the flow definition, not over
//! GStreamer state — so the failure paths the spec names are unit-testable
//! without a running pipeline. Execution lives in the caller.

use crate::blocks::builtin::mediaplayer::{MediaPlayerKey, MEDIA_PLAYER_REGISTRY};
use crate::blocks::builtin::vision_mixer::properties::{
    parse_dsk_alpha_modes, parse_num_dsk_inputs, parse_num_inputs,
};
use crate::gst::pipeline::PipelineManager;
use strom_types::element::Link;
use strom_types::vision_mixer::AlphaMode;
use strom_types::Flow;
use strom_types::{BlockInstance, FlowId};
use tracing::{info, warn};

/// Pad name a stinger source exposes its video on.
const SOURCE_PAD: &str = "video_out";

/// Property by which a media player declares itself a stinger clip source.
///
/// Wiring alone is deliberately not enough: a media player may sit on a keyed
/// input to play a looping graphic, and arming it would park it on frame 0 and
/// stop it looping. Declaration keeps stinger behaviour opt-in.
pub const STINGER_SOURCE_PROPERTY: &str = "stinger_source";

/// Property on a clip source declaring how its clip's alpha is encoded.
/// Absent means straight. It has to match the keyed input it is wired to.
pub const ALPHA_MODE_PROPERTY: &str = "alpha_mode";

/// Block definition of an HTML graphic, whose page is a stinger's overlay.
pub const HTML_GRAPHIC_BLOCK: &str = "builtin.html_graphic";

/// How long a web stinger's animation runs. A page has no length of its own, so
/// it is declared on the block.
pub const DURATION_PROPERTY: &str = "stinger_duration_ms";

/// Timing properties, which live on the clip source rather than on the take.
/// When the clip fully covers the frame is a property of the artwork, so the
/// person who cut the clip sets it once instead of the operator retyping it
/// under time pressure.
pub const CUT_POINT_PROPERTY: &str = "stinger_cut_point_ms";
pub const UNDER_TRANSITION_PROPERTY: &str = "stinger_under_transition";
pub const UNDER_DURATION_PROPERTY: &str = "stinger_under_duration_ms";

/// Transitions a stinger can run beneath its overlay, as `(value, label)`.
const UNDER_TRANSITION_CHOICES: &[(&str, &str)] = &[
    ("cut", "Cut"),
    ("fade", "Mix"),
    ("dip_to_black", "Dip to Black"),
    ("wipe_left", "Wipe Left"),
    ("wipe_right", "Wipe Right"),
    ("wipe_up", "Wipe Up"),
    ("wipe_down", "Wipe Down"),
];

/// The transitions beneath a stinger, for a source block's property enum.
pub fn under_transition_enum_values() -> Vec<strom_types::block::EnumValue> {
    UNDER_TRANSITION_CHOICES
        .iter()
        .map(|(value, label)| strom_types::block::EnumValue {
            value: value.to_string(),
            label: Some(label.to_string()),
        })
        .collect()
}

/// What plays a stinger's overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StingerSourceKind {
    /// A media player clip, which is as long as its file.
    Clip,
    /// An HTML graphic, whose page runs for the duration declared on the block.
    Web { duration_ms: u64 },
}

/// Which keyed input of a mixer a stinger source feeds, and the timing the
/// source itself declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StingerBinding {
    /// Block id of the media player or HTML graphic supplying the overlay.
    pub source_block_id: String,
    pub kind: StingerSourceKind,
    /// Index of the mixer's keyed (DSK) input it is wired to.
    pub dsk_index: usize,
    /// How far into the clip the program source changes. `None` means the clip
    /// did not declare one, and the halfway point is used.
    pub cut_point_ms: Option<u64>,
    /// Transition running beneath the clip while it covers the frame.
    pub under_transition: String,
    /// How long that transition takes, shortened if it would outlast the clip.
    pub under_duration_ms: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StingerError {
    #[error("stinger requires a clip source, but none was named")]
    MissingSource,
    #[error("no block '{0}' in this flow to use as a stinger clip source")]
    UnknownSource(String),
    #[error("no vision mixer '{0}' in this flow")]
    UnknownMixer(String),
    #[error(
        "block '{0}' is not declared as a stinger clip source — enable that on the \
         block so its clip is held ready, or it would fire late"
    )]
    SourceNotDeclared(String),
    #[error(
        "block '{source_block}' is not wired to a keyed (DSK) input of mixer \
         '{mixer}', so it cannot be used as a stinger"
    )]
    SourceNotKeyed { source_block: String, mixer: String },
    #[error("mixer '{0}' has no keyed (DSK) inputs configured, so it cannot play a stinger")]
    NoKeyedInputs(String),
    #[error(
        "stinger source '{source_block}' has {source_mode} alpha but keyed input \
         {dsk_number} of mixer '{mixer}' is declared {input_mode} — set that input's \
         alpha mode to {source_mode}, or it composites too dark or too bright"
    )]
    AlphaModeMismatch {
        source_block: String,
        mixer: String,
        dsk_number: usize,
        source_mode: AlphaMode,
        input_mode: AlphaMode,
    },
    #[error(
        "HTML graphic '{0}' has no stinger duration — a page has no length of its \
         own, so set how long its animation runs"
    )]
    WebSourceNeedsDuration(String),
    #[error(
        "stinger cut point {cut_point_ms} ms is at or beyond the clip length \
         {clip_ms} ms, so the transition beneath would never run"
    )]
    CutPointBeyondClip { cut_point_ms: u64, clip_ms: u64 },
    #[error("a stinger is already running on mixer '{0}'")]
    AlreadyRunning(String),
    #[error("clip source '{0}' has no clip loaded")]
    NoClipLoaded(String),
    #[error("unknown transition '{0}' requested beneath the stinger")]
    UnknownUnderTransition(String),
    #[error("a stinger cannot run beneath a stinger")]
    StingerBeneathStinger,
}

/// Whether a block declares itself a stinger clip source.
///
/// Defaults to false: a player must opt in, so wiring a looping graphic to a
/// keyed input never causes it to be parked or unlooped.
pub fn declares_stinger_source(block: &BlockInstance) -> bool {
    matches!(
        block.properties.get(STINGER_SOURCE_PROPERTY),
        Some(strom_types::PropertyValue::Bool(true))
    )
}

/// Read a non-negative integer property, whichever numeric variant it was
/// stored as. A flow round-tripped through JSON may carry either.
fn read_u64(block: &BlockInstance, name: &str) -> Option<u64> {
    match block.properties.get(name)? {
        strom_types::PropertyValue::UInt(v) => Some(*v),
        strom_types::PropertyValue::Int(v) => u64::try_from(*v).ok(),
        _ => None,
    }
}

/// How a stinger source encodes alpha. Chromium always paints premultiplied, so
/// an HTML graphic is premultiplied whatever it declares; a clip is what its
/// `alpha_mode` says, straight when unset.
fn source_alpha_mode(block: &BlockInstance) -> AlphaMode {
    if block.block_definition_id == HTML_GRAPHIC_BLOCK {
        return AlphaMode::Premultiplied;
    }
    match block.properties.get(ALPHA_MODE_PROPERTY) {
        Some(strom_types::PropertyValue::String(mode)) => mode.parse().unwrap_or_default(),
        _ => AlphaMode::default(),
    }
}

/// Strip any element suffix from a block instance id (`"mixer1:mixer"` ->
/// `"mixer1"`), which is how ids appear in a flow's links.
fn block_id_of(instance_id: &str) -> &str {
    instance_id.split(':').next().unwrap_or(instance_id)
}

/// Work out which keyed input `source_block_id` feeds on `mixer_instance_id`.
///
/// Compares against generated pad names rather than splitting `id:pad`, because
/// a block id may itself contain a colon.
pub fn resolve_binding(
    blocks: &[BlockInstance],
    links: &[Link],
    mixer_instance_id: &str,
    source_block_id: Option<&str>,
) -> Result<StingerBinding, StingerError> {
    let source = source_block_id
        .filter(|s| !s.is_empty())
        .ok_or(StingerError::MissingSource)?;
    let mixer = block_id_of(mixer_instance_id);

    let mixer_block = blocks
        .iter()
        .find(|b| b.id == mixer)
        .ok_or_else(|| StingerError::UnknownMixer(mixer.to_string()))?;
    let num_dsk = parse_num_dsk_inputs(&mixer_block.properties);
    if num_dsk == 0 {
        return Err(StingerError::NoKeyedInputs(mixer.to_string()));
    }

    let source_block = blocks
        .iter()
        .find(|b| b.id == source)
        .ok_or_else(|| StingerError::UnknownSource(source.to_string()))?;

    if !declares_stinger_source(source_block) {
        return Err(StingerError::SourceNotDeclared(source.to_string()));
    }

    let kind = if source_block.block_definition_id == HTML_GRAPHIC_BLOCK {
        let duration_ms = read_u64(source_block, DURATION_PROPERTY).unwrap_or(0);
        if duration_ms == 0 {
            return Err(StingerError::WebSourceNeedsDuration(source.to_string()));
        }
        StingerSourceKind::Web { duration_ms }
    } else {
        StingerSourceKind::Clip
    };
    let source_mode = source_alpha_mode(source_block);
    let input_modes = parse_dsk_alpha_modes(&mixer_block.properties, num_dsk);

    let from = format!("{source}:{SOURCE_PAD}");
    for (idx, &input_mode) in input_modes.iter().enumerate() {
        let to = format!("{mixer}:dsk_in_{idx}");
        if links.iter().any(|l| l.from == from && l.to == to) {
            if input_mode != source_mode {
                return Err(StingerError::AlphaModeMismatch {
                    source_block: source.to_string(),
                    mixer: mixer.to_string(),
                    dsk_number: idx + 1,
                    source_mode,
                    input_mode,
                });
            }
            return Ok(StingerBinding {
                source_block_id: source.to_string(),
                kind,
                dsk_index: idx,
                cut_point_ms: read_u64(source_block, CUT_POINT_PROPERTY).filter(|ms| *ms > 0),
                under_transition: match source_block.properties.get(UNDER_TRANSITION_PROPERTY) {
                    Some(strom_types::PropertyValue::String(s)) if !s.is_empty() => s.clone(),
                    _ => strom_types::api::DEFAULT_STINGER_UNDER_TRANSITION.to_string(),
                },
                under_duration_ms: {
                    let ms = read_u64(source_block, UNDER_DURATION_PROPERTY).unwrap_or(0);
                    let under = source_block.properties.get(UNDER_TRANSITION_PROPERTY);
                    if ms == 0
                        && matches!(under, Some(strom_types::PropertyValue::String(t))
                            if !t.eq_ignore_ascii_case("cut") && !t.is_empty())
                    {
                        warn!(
                            "Stinger source {source}: '{}' beneath the clip has no duration, \
                             so it will run as a cut",
                            under.map(|u| format!("{u:?}")).unwrap_or_default()
                        );
                    }
                    ms
                },
            });
        }
    }

    Err(StingerError::SourceNotKeyed {
        source_block: source.to_string(),
        mixer: mixer.to_string(),
    })
}

/// Check the cut point falls inside the clip, and shorten the underlying
/// transition if it would outlast the clip.
///
/// Returns the duration to actually use, and `Some(requested)` when it had to
/// be shortened so the caller can warn with both numbers.
pub fn fit_under_transition(
    cut_point_ms: u64,
    requested_duration_ms: u64,
    clip_ms: u64,
) -> Result<(u64, Option<u64>), StingerError> {
    if clip_ms == 0 || cut_point_ms >= clip_ms {
        return Err(StingerError::CutPointBeyondClip {
            cut_point_ms,
            clip_ms,
        });
    }
    let remaining = clip_ms - cut_point_ms;
    if requested_duration_ms > remaining {
        Ok((remaining, Some(requested_duration_ms)))
    } else {
        Ok((requested_duration_ms, None))
    }
}

/// Where a web stinger's animation starts, in the mixer's timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebStart {
    /// Too soon to tell: keep waiting for the page's first frame.
    Waiting,
    /// The page's first frame arrived promptly, and its timestamp is the start.
    FirstFrame(u64),
    /// No prompt frame, so the start is estimated from when the take changed the
    /// URL. The page may have painted nothing visible at first, or be slow.
    FromTake(u64),
}

/// Decide where a web stinger's animation starts.
///
/// Chromium delivers a frame only when pixels change, so a page's first frame
/// marks the start of its animation only if the animation changes something on
/// screen straight away. One that opens on invisible frames (an element moving
/// in from off frame, a fade from nothing, a delayed start) delivers its first
/// frame late, and anchoring on it would cut late by the same amount. Its own
/// clock started at `hashchange`, so a frame stamped more than `grace_ns` after
/// the take is ignored in favour of the take plus the usual delivery delay.
///
/// All times are running times in nanoseconds. `first_frame_ns` is the first
/// frame of new content stamped at or after `taken_at_ns`. `output_ns` is the
/// newest timestamp seen on the output, repeats included: the grace period ends
/// when the output has moved past it, not when wall time has, because frames
/// can be held for a while between being painted and reaching the output.
pub fn web_stinger_start(
    first_frame_ns: Option<u64>,
    taken_at_ns: u64,
    output_ns: Option<u64>,
    grace_ns: u64,
    delay_ns: u64,
) -> WebStart {
    let deadline = taken_at_ns.saturating_add(grace_ns);
    match first_frame_ns {
        Some(frame) if frame <= deadline => WebStart::FirstFrame(frame),
        _ if output_ns.is_some_and(|out| out > deadline) => {
            WebStart::FromTake(taken_at_ns.saturating_add(delay_ns))
        }
        _ => WebStart::Waiting,
    }
}

/// The output frame a web stinger's first frame is taken to start on: the one
/// whose start is nearest its timestamp.
///
/// Which output frame first composites a page frame depends on when it reaches
/// the mixer, which varies with how its paint falls against livesync's slots
/// and the mixer's frames. Anchoring on the frame containing the timestamp cut
/// about a frame early on average and on the next frame about a frame late;
/// the nearest keeps the error within half a frame either way.
pub fn web_first_output_frame(first_frame_ns: u64, frame_ns: u64) -> u64 {
    (first_frame_ns + frame_ns / 2) / frame_ns * frame_ns
}

/// Keyed inputs a block feeds, as `(mixer block id, keyed input index)`.
///
/// Compares against generated pad names rather than splitting on ':', because a
/// block id may itself contain a colon.
fn keyed_inputs_fed_by<'a>(
    blocks: &'a [BlockInstance],
    links: &[Link],
    source_block_id: &str,
) -> Vec<(&'a str, usize)> {
    let from = format!("{source_block_id}:{SOURCE_PAD}");
    let mut fed = Vec::new();
    for mixer in blocks
        .iter()
        .filter(|b| b.block_definition_id == "builtin.vision_mixer")
    {
        for idx in 0..parse_num_dsk_inputs(&mixer.properties) {
            let to = format!("{}:dsk_in_{idx}", mixer.id);
            if links.iter().any(|l| l.from == from && l.to == to) {
                fed.push((mixer.id.as_str(), idx));
            }
        }
    }
    fed
}

/// Prepare every declared stinger source in this flow, and the keyed inputs it
/// feeds, so a take fires cleanly.
///
/// Must run after the pipeline is up: the media player auto-plays, so a source
/// that is merely built is not on frame 0, and the keyed pads do not exist
/// before the mixer is built. Undeclared sources are untouched, which is what
/// keeps a looping graphic on a keyed input playing.
pub fn prepare_declared_sources(flow_id: FlowId, flow: &Flow, manager: &PipelineManager) {
    warn_on_alpha_mismatches(flow);

    for block in flow.blocks.iter().filter(|b| declares_stinger_source(b)) {
        // A page is already running and idle when the flow starts, and its last
        // paint is transparent, so there is nothing to park and nothing stale
        // for its keyed input to hold.
        if block.block_definition_id == HTML_GRAPHIC_BLOCK {
            continue;
        }

        let key = MediaPlayerKey {
            flow_id,
            block_id: block.id.clone(),
        };
        match MEDIA_PLAYER_REGISTRY.get(&key) {
            Some(player) => match player.arm_stinger() {
                Ok(()) => info!("Stinger source {} armed on its first frame", block.id),
                Err(e) => warn!(
                    "Stinger source {} could not be armed ({}) — its first fire will be late",
                    block.id, e
                ),
            },
            None => warn!(
                "Block {} declares itself a stinger source but no media player is \
                 registered for it",
                block.id
            ),
        }

        // A keyed input holds its last frame by default, which for a stinger
        // means the frame its clip ended on is composited the moment the input
        // is revealed for the next take. Only inputs a declared stinger source
        // feeds are changed; holding is right for anything meant to stay on
        // screen.
        for (mixer_id, dsk_index) in keyed_inputs_fed_by(&flow.blocks, &flow.links, &block.id) {
            let num_inputs = flow
                .blocks
                .iter()
                .find(|b| b.id == mixer_id)
                .map(|b| parse_num_inputs(&b.properties))
                .unwrap_or(0);
            if let Err(e) = manager.set_dsk_hold_last_frame(mixer_id, dsk_index, num_inputs, false)
            {
                warn!(
                    "Keyed input {} of {} could not be told to drop its last frame ({}) \
                     — a stinger there may flash the end of the previous clip",
                    dsk_index, mixer_id, e
                );
            }
        }
    }
}

/// Keyed inputs whose declared alpha mode disagrees with the source feeding
/// them, as `(source, mixer, keyed input index, source mode, input mode)`.
/// Checks every HTML graphic and every block declaring `alpha_mode`, stinger
/// source or not.
pub fn alpha_mismatches(flow: &Flow) -> Vec<(String, String, usize, AlphaMode, AlphaMode)> {
    let mut out = Vec::new();
    for source in flow.blocks.iter().filter(|b| {
        b.block_definition_id == HTML_GRAPHIC_BLOCK
            || b.properties.contains_key(ALPHA_MODE_PROPERTY)
    }) {
        let source_mode = source_alpha_mode(source);
        for (mixer_id, dsk_index) in keyed_inputs_fed_by(&flow.blocks, &flow.links, &source.id) {
            let Some(mixer) = flow.blocks.iter().find(|b| b.id == mixer_id) else {
                continue;
            };
            let modes = parse_dsk_alpha_modes(&mixer.properties, dsk_index + 1);
            if modes[dsk_index] != source_mode {
                out.push((
                    source.id.clone(),
                    mixer_id.to_string(),
                    dsk_index,
                    source_mode,
                    modes[dsk_index],
                ));
            }
        }
    }
    out
}

fn warn_on_alpha_mismatches(flow: &Flow) {
    for (source, mixer, dsk_index, source_mode, input_mode) in alpha_mismatches(flow) {
        warn!(
            "{} has {} alpha but feeds keyed input {} of {}, which is declared {} — it \
             will composite too dark or too bright until the input's alpha mode matches",
            source,
            source_mode,
            dsk_index + 1,
            mixer,
            input_mode
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use strom_types::{block::Position, PropertyValue};

    fn block(id: &str, def: &str, props: &[(&str, PropertyValue)]) -> BlockInstance {
        BlockInstance {
            id: id.to_string(),
            block_definition_id: def.to_string(),
            name: None,
            properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<HashMap<_, _>>(),
            position: Position { x: 0.0, y: 0.0 },
            runtime_data: None,
            computed_external_pads: None,
        }
    }

    fn link(from: &str, to: &str) -> Link {
        Link {
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    fn mixer_with_dsk(n: u32) -> BlockInstance {
        block(
            "mixer1",
            "builtin.vision_mixer",
            &[("num_dsk_inputs", PropertyValue::UInt(n as u64))],
        )
    }

    fn media_player() -> BlockInstance {
        block(
            "mp1",
            "builtin.media_player",
            &[(STINGER_SOURCE_PROPERTY, PropertyValue::Bool(true))],
        )
    }

    /// Wired to a keyed input, but not declared — e.g. a looping graphic.
    fn undeclared_player() -> BlockInstance {
        block("mp1", "builtin.media_player", &[])
    }

    #[test]
    fn resolves_the_keyed_input_a_source_feeds() {
        let blocks = vec![mixer_with_dsk(2), media_player()];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_1")];
        assert_eq!(
            resolve_binding(&blocks, &links, "mixer1", Some("mp1")).unwrap(),
            StingerBinding {
                source_block_id: "mp1".to_string(),
                kind: StingerSourceKind::Clip,
                dsk_index: 1,
                cut_point_ms: None,
                under_transition: "cut".to_string(),
                under_duration_ms: 0,
            }
        );
    }

    /// Cut point and the transition beneath ride on the clip source block, so
    /// a take only has to name the clip.
    #[test]
    fn timing_comes_from_the_clip_source_block() {
        let blocks = vec![
            mixer_with_dsk(1),
            block(
                "mp1",
                "builtin.media_player",
                &[
                    (STINGER_SOURCE_PROPERTY, PropertyValue::Bool(true)),
                    (CUT_POINT_PROPERTY, PropertyValue::UInt(700)),
                    (
                        UNDER_TRANSITION_PROPERTY,
                        PropertyValue::String("wipe_left".to_string()),
                    ),
                    (UNDER_DURATION_PROPERTY, PropertyValue::UInt(250)),
                ],
            ),
        ];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        let binding = resolve_binding(&blocks, &links, "mixer1", Some("mp1")).unwrap();
        assert_eq!(binding.cut_point_ms, Some(700));
        assert_eq!(binding.under_transition, "wipe_left");
        assert_eq!(binding.under_duration_ms, 250);
    }

    /// A cut point of 0 means "not declared", so the halfway point is used
    /// rather than cutting on the clip's first frame.
    #[test]
    fn a_zero_cut_point_reads_as_undeclared() {
        let blocks = vec![
            mixer_with_dsk(1),
            block(
                "mp1",
                "builtin.media_player",
                &[
                    (STINGER_SOURCE_PROPERTY, PropertyValue::Bool(true)),
                    (CUT_POINT_PROPERTY, PropertyValue::UInt(0)),
                ],
            ),
        ];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        let binding = resolve_binding(&blocks, &links, "mixer1", Some("mp1")).unwrap();
        assert_eq!(binding.cut_point_ms, None);
    }

    #[test]
    fn resolves_when_the_mixer_id_carries_an_element_suffix() {
        let blocks = vec![mixer_with_dsk(1), media_player()];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        assert!(resolve_binding(&blocks, &links, "mixer1:mixer", Some("mp1")).is_ok());
    }

    #[test]
    fn unknown_source_is_rejected() {
        let blocks = vec![mixer_with_dsk(1)];
        assert_eq!(
            resolve_binding(&blocks, &[], "mixer1", Some("nope")),
            Err(StingerError::UnknownSource("nope".to_string()))
        );
    }

    #[test]
    fn source_not_wired_to_a_keyed_input_is_rejected() {
        let blocks = vec![mixer_with_dsk(2), media_player()];
        // Wired to a normal video input, not a keyed one.
        let links = vec![link("mp1:video_out", "mixer1:video_in_0")];
        assert_eq!(
            resolve_binding(&blocks, &links, "mixer1", Some("mp1")),
            Err(StingerError::SourceNotKeyed {
                source_block: "mp1".to_string(),
                mixer: "mixer1".to_string(),
            })
        );
    }

    #[test]
    fn mixer_without_keyed_inputs_is_rejected() {
        let blocks = vec![mixer_with_dsk(0), media_player()];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        assert_eq!(
            resolve_binding(&blocks, &links, "mixer1", Some("mp1")),
            Err(StingerError::NoKeyedInputs("mixer1".to_string()))
        );
    }

    #[test]
    fn missing_source_is_rejected() {
        let blocks = vec![mixer_with_dsk(1)];
        assert_eq!(
            resolve_binding(&blocks, &[], "mixer1", None),
            Err(StingerError::MissingSource)
        );
        assert_eq!(
            resolve_binding(&blocks, &[], "mixer1", Some("")),
            Err(StingerError::MissingSource)
        );
    }

    #[test]
    fn source_wired_but_not_declared_is_rejected() {
        let blocks = vec![mixer_with_dsk(1), undeclared_player()];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        assert_eq!(
            resolve_binding(&blocks, &links, "mixer1", Some("mp1")),
            Err(StingerError::SourceNotDeclared("mp1".to_string()))
        );
    }

    #[test]
    fn declaration_defaults_to_false() {
        assert!(!declares_stinger_source(&undeclared_player()));
        assert!(declares_stinger_source(&media_player()));
    }

    /// A premultiplied clip on an input declared straight would composite too
    /// dark, so it is refused before anything plays.
    #[test]
    fn premultiplied_clip_on_a_straight_input_is_rejected() {
        let blocks = vec![
            mixer_with_dsk(1),
            block(
                "mp1",
                "builtin.media_player",
                &[
                    (STINGER_SOURCE_PROPERTY, PropertyValue::Bool(true)),
                    (
                        ALPHA_MODE_PROPERTY,
                        PropertyValue::String("premultiplied".to_string()),
                    ),
                ],
            ),
        ];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        assert_eq!(
            resolve_binding(&blocks, &links, "mixer1", Some("mp1")),
            Err(StingerError::AlphaModeMismatch {
                source_block: "mp1".to_string(),
                mixer: "mixer1".to_string(),
                dsk_number: 1,
                source_mode: AlphaMode::Premultiplied,
                input_mode: AlphaMode::Straight,
            })
        );
    }

    #[test]
    fn premultiplied_clip_on_a_premultiplied_input_is_accepted() {
        let mut mixer = mixer_with_dsk(1);
        mixer.properties.insert(
            "dsk_0_alpha_mode".to_string(),
            PropertyValue::String("premultiplied".to_string()),
        );
        let blocks = vec![
            mixer,
            block(
                "mp1",
                "builtin.media_player",
                &[
                    (STINGER_SOURCE_PROPERTY, PropertyValue::Bool(true)),
                    (
                        ALPHA_MODE_PROPERTY,
                        PropertyValue::String("premultiplied".to_string()),
                    ),
                ],
            ),
        ];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        assert!(resolve_binding(&blocks, &links, "mixer1", Some("mp1")).is_ok());
    }

    fn html_graphic(props: &[(&str, PropertyValue)]) -> BlockInstance {
        let mut all = vec![(STINGER_SOURCE_PROPERTY, PropertyValue::Bool(true))];
        all.extend(props.iter().cloned());
        block("web1", HTML_GRAPHIC_BLOCK, &all)
    }

    fn premultiplied_mixer() -> BlockInstance {
        let mut mixer = mixer_with_dsk(1);
        mixer.properties.insert(
            "dsk_0_alpha_mode".to_string(),
            PropertyValue::String("premultiplied".to_string()),
        );
        mixer
    }

    #[test]
    fn a_web_source_takes_its_length_from_the_block() {
        let blocks = vec![
            premultiplied_mixer(),
            html_graphic(&[
                (DURATION_PROPERTY, PropertyValue::UInt(1200)),
                (CUT_POINT_PROPERTY, PropertyValue::UInt(500)),
            ]),
        ];
        let links = vec![link("web1:video_out", "mixer1:dsk_in_0")];
        let binding = resolve_binding(&blocks, &links, "mixer1", Some("web1")).unwrap();
        assert_eq!(binding.kind, StingerSourceKind::Web { duration_ms: 1200 });
        assert_eq!(binding.cut_point_ms, Some(500));
    }

    #[test]
    fn a_web_source_without_a_duration_is_rejected() {
        let blocks = vec![premultiplied_mixer(), html_graphic(&[])];
        let links = vec![link("web1:video_out", "mixer1:dsk_in_0")];
        assert_eq!(
            resolve_binding(&blocks, &links, "mixer1", Some("web1")),
            Err(StingerError::WebSourceNeedsDuration("web1".to_string()))
        );
    }

    /// Chromium paints premultiplied, so a page is premultiplied even when the
    /// block says otherwise, and a straight input refuses it.
    #[test]
    fn a_web_source_on_a_straight_input_is_rejected() {
        let blocks = vec![
            mixer_with_dsk(1),
            html_graphic(&[
                (DURATION_PROPERTY, PropertyValue::UInt(1000)),
                (
                    ALPHA_MODE_PROPERTY,
                    PropertyValue::String("straight".to_string()),
                ),
            ]),
        ];
        let links = vec![link("web1:video_out", "mixer1:dsk_in_0")];
        assert!(matches!(
            resolve_binding(&blocks, &links, "mixer1", Some("web1")),
            Err(StingerError::AlphaModeMismatch {
                source_mode: AlphaMode::Premultiplied,
                input_mode: AlphaMode::Straight,
                ..
            })
        ));
    }

    /// A mismatch is reported for any HTML graphic, not just a stinger source.
    #[test]
    fn alpha_mismatches_cover_graphics_that_are_not_stingers() {
        let flow = Flow {
            blocks: vec![
                mixer_with_dsk(2),
                block("lower_third", HTML_GRAPHIC_BLOCK, &[]),
                html_graphic(&[(DURATION_PROPERTY, PropertyValue::UInt(1000))]),
            ],
            links: vec![
                link("lower_third:video_out", "mixer1:dsk_in_0"),
                link("web1:video_out", "mixer1:dsk_in_1"),
            ],
            ..Flow::new("alpha")
        };
        let mut flagged: Vec<_> = alpha_mismatches(&flow)
            .into_iter()
            .map(|(source, _, idx, _, _)| (source, idx))
            .collect();
        flagged.sort();
        assert_eq!(
            flagged,
            vec![("lower_third".to_string(), 0), ("web1".to_string(), 1)]
        );
    }

    #[test]
    fn straight_alpha_declared_explicitly_is_accepted() {
        let blocks = vec![
            mixer_with_dsk(1),
            block(
                "mp1",
                "builtin.media_player",
                &[
                    (STINGER_SOURCE_PROPERTY, PropertyValue::Bool(true)),
                    (
                        ALPHA_MODE_PROPERTY,
                        PropertyValue::String("straight".to_string()),
                    ),
                ],
            ),
        ];
        let links = vec![link("mp1:video_out", "mixer1:dsk_in_0")];
        assert!(resolve_binding(&blocks, &links, "mixer1", Some("mp1")).is_ok());
    }

    const MS: u64 = 1_000_000;

    #[test]
    fn a_prompt_first_frame_marks_the_start() {
        assert_eq!(
            web_stinger_start(
                Some(1_060 * MS),
                1_000 * MS,
                Some(1_060 * MS),
                100 * MS,
                45 * MS
            ),
            WebStart::FirstFrame(1_060 * MS)
        );
    }

    #[test]
    fn no_frame_yet_within_the_grace_period_keeps_waiting() {
        assert_eq!(
            web_stinger_start(None, 1_000 * MS, Some(1_090 * MS), 100 * MS, 45 * MS),
            WebStart::Waiting
        );
    }

    /// A frame painted inside the grace period but held on its way to the
    /// output still counts once it arrives: the wait is over only when the
    /// output has moved past the grace period, however long that takes.
    #[test]
    fn waiting_ends_by_output_timestamps_not_wall_time() {
        assert_eq!(
            web_stinger_start(None, 1_000 * MS, None, 100 * MS, 45 * MS),
            WebStart::Waiting
        );
        assert_eq!(
            web_stinger_start(
                Some(1_063 * MS),
                1_000 * MS,
                Some(1_063 * MS),
                100 * MS,
                45 * MS
            ),
            WebStart::FirstFrame(1_063 * MS)
        );
    }

    /// A page that opens on invisible frames paints its first frame late; the
    /// cut must not move with it.
    #[test]
    fn a_late_first_frame_is_replaced_by_the_take_time() {
        assert_eq!(
            web_stinger_start(
                Some(1_300 * MS),
                1_000 * MS,
                Some(1_300 * MS),
                100 * MS,
                45 * MS
            ),
            WebStart::FromTake(1_045 * MS)
        );
    }

    #[test]
    fn no_frame_once_the_output_passes_the_grace_period_falls_back_to_the_take() {
        assert_eq!(
            web_stinger_start(None, 1_000 * MS, Some(1_101 * MS), 100 * MS, 45 * MS),
            WebStart::FromTake(1_045 * MS)
        );
    }

    const FRAME: u64 = 33_333_333;

    #[test]
    fn a_page_frame_late_in_an_output_frame_is_taken_to_start_on_the_next() {
        assert_eq!(
            web_first_output_frame(29 * FRAME + 20 * MS, FRAME),
            30 * FRAME
        );
    }

    #[test]
    fn a_page_frame_early_in_an_output_frame_is_taken_to_start_on_it() {
        assert_eq!(
            web_first_output_frame(29 * FRAME + 10 * MS, FRAME),
            29 * FRAME
        );
    }

    #[test]
    fn cut_point_inside_the_clip_keeps_the_requested_duration() {
        assert_eq!(fit_under_transition(400, 300, 2000).unwrap(), (300, None));
    }

    #[test]
    fn cut_point_at_or_beyond_the_clip_is_rejected() {
        assert_eq!(
            fit_under_transition(2000, 300, 2000),
            Err(StingerError::CutPointBeyondClip {
                cut_point_ms: 2000,
                clip_ms: 2000,
            })
        );
        assert!(fit_under_transition(2500, 300, 2000).is_err());
        // An unknown clip length cannot be validated against.
        assert!(fit_under_transition(0, 300, 0).is_err());
    }

    #[test]
    fn under_transition_outlasting_the_clip_is_shortened() {
        // 1800 ms in, 300 ms of clip left, 500 ms requested.
        assert_eq!(
            fit_under_transition(1800, 500, 2100).unwrap(),
            (300, Some(500))
        );
    }
}
