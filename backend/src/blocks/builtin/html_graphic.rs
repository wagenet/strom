//! HTML graphic block: a web page rendered by `cefsrc` as a keyed overlay.
//!
//! Chain: `cefsrc -> capsfilter`. `cefsrc` has no width or height properties and
//! renders at whatever size is negotiated, so the capsfilter sets it. Its caps
//! carry a variable frame rate (`framerate=0/1`); `max-video-framerate` caps how
//! often Chromium paints, and nothing downstream may pin a fixed rate or
//! negotiation fails.
//!
//! Chromium paints premultiplied alpha, so a keyed input fed by this block must
//! be declared premultiplied. As a stinger source, a take changes the page's URL
//! fragment and the animation runs for the duration declared on the block.

use crate::blocks::{BlockBuildContext, BlockBuildError, BlockBuildResult, BlockBuilder};
use crate::gst::stinger;
use gstreamer as gst;
use gstreamer::prelude::*;
use std::collections::HashMap;
use strom_types::{
    block::*, common_video_framerate_enum_values, common_video_resolution_enum_values,
    PropertyValue, *,
};
use tracing::info;

pub const DEFAULT_RESOLUTION: &str = "1920x1080";
pub const DEFAULT_FRAMERATE: &str = "30/1";

/// Element id suffix of the block's `cefsrc`.
pub const CEFSRC_ELEMENT: &str = "cefsrc";
/// Element id suffix of the element whose src pad is the block's output.
pub const OUTPUT_ELEMENT: &str = "capsfilter";

/// HTML graphic block builder.
pub struct HtmlGraphicBuilder;

fn string_prop<'a>(properties: &'a HashMap<String, PropertyValue>, name: &str) -> Option<&'a str> {
    match properties.get(name) {
        Some(PropertyValue::String(s)) if !s.trim().is_empty() => Some(s.trim()),
        _ => None,
    }
}

/// Parse `WIDTHxHEIGHT`.
fn parse_resolution(value: &str) -> Option<(i32, i32)> {
    let (w, h) = value.split_once('x')?;
    let (w, h) = (w.trim().parse().ok()?, h.trim().parse().ok()?);
    (w > 0 && h > 0).then_some((w, h))
}

/// Parse `NUMER/DENOM`, or a bare integer rate.
fn parse_fraction(value: &str) -> Option<(i32, i32)> {
    let (n, d) = match value.split_once('/') {
        Some((n, d)) => (n.trim().parse().ok()?, d.trim().parse().ok()?),
        None => (value.trim().parse().ok()?, 1),
    };
    (n > 0 && d > 0).then_some((n, d))
}

impl BlockBuilder for HtmlGraphicBuilder {
    fn build(
        &self,
        instance_id: &str,
        properties: &HashMap<String, PropertyValue>,
        _ctx: &BlockBuildContext,
    ) -> Result<BlockBuildResult, BlockBuildError> {
        let url = string_prop(properties, "url").ok_or_else(|| {
            BlockBuildError::InvalidConfiguration("HTML graphic needs a URL".to_string())
        })?;
        let resolution = string_prop(properties, "resolution").unwrap_or(DEFAULT_RESOLUTION);
        let (width, height) = parse_resolution(resolution).ok_or_else(|| {
            BlockBuildError::InvalidConfiguration(format!(
                "HTML graphic resolution '{resolution}' is not WIDTHxHEIGHT"
            ))
        })?;
        let framerate = string_prop(properties, "framerate").unwrap_or(DEFAULT_FRAMERATE);

        info!(
            "Building HTML graphic {}: {} at {}x{}, up to {} fps",
            instance_id, url, width, height, framerate
        );

        let cefsrc_id = format!("{instance_id}:{CEFSRC_ELEMENT}");
        let caps_id = format!("{instance_id}:{OUTPUT_ELEMENT}");

        let cefsrc = gst::ElementFactory::make("cefsrc")
            .name(&cefsrc_id)
            .property("url", url)
            .build()
            .map_err(|e| {
                BlockBuildError::ElementCreation(format!(
                    "cefsrc ({e}) — HTML graphics need the gstcefsrc plugin, which the \
                     strom-full image provides"
                ))
            })?;
        let (numer, denom) = parse_fraction(framerate).ok_or_else(|| {
            BlockBuildError::InvalidConfiguration(format!(
                "HTML graphic framerate '{framerate}' is not a fraction like 30/1"
            ))
        })?;
        cefsrc.set_property("max-video-framerate", gst::Fraction::new(numer, denom));

        let caps = gst::Caps::builder("video/x-raw")
            .field("width", width)
            .field("height", height)
            .build();
        let capsfilter = gst::ElementFactory::make("capsfilter")
            .name(&caps_id)
            .property("caps", &caps)
            .build()
            .map_err(|e| BlockBuildError::ElementCreation(format!("capsfilter: {e}")))?;

        Ok(BlockBuildResult {
            elements: vec![(cefsrc_id.clone(), cefsrc), (caps_id.clone(), capsfilter)],
            internal_links: vec![(
                element::ElementPadRef::pad(&cefsrc_id, "src"),
                element::ElementPadRef::pad(&caps_id, "sink"),
            )],
            bus_message_handler: None,
            pad_properties: HashMap::new(),
        })
    }
}

/// Get metadata for the HTML graphic block (for UI/API).
pub fn get_blocks() -> Vec<BlockDefinition> {
    vec![html_graphic_definition()]
}

fn block_property(
    name: &str,
    label: &str,
    description: &str,
    property_type: PropertyType,
    default_value: PropertyValue,
) -> ExposedProperty {
    ExposedProperty {
        name: name.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        property_type,
        default_value: Some(default_value),
        mapping: PropertyMapping {
            element_id: "_block".to_string(),
            property_name: name.to_string(),
            transform: None,
        },
        live: false,
        persist: None,
    }
}

fn html_graphic_definition() -> BlockDefinition {
    BlockDefinition {
        id: stinger::HTML_GRAPHIC_BLOCK.to_string(),
        name: "HTML Graphic".to_string(),
        description: "Renders a web page as a keyed video overlay. Chromium paints premultiplied alpha, so wire it to a keyed input declared premultiplied. Requires the gstcefsrc plugin.".to_string(),
        category: "Inputs".to_string(),
        exposed_properties: vec![
            block_property(
                "url",
                "URL",
                "Page to render (http://, https://, file:// or data:). Give it a transparent background.",
                PropertyType::String,
                PropertyValue::String(String::new()),
            ),
            block_property(
                "resolution",
                "Resolution",
                "Size Chromium renders the page at. Rendering cost scales with pixel count.",
                PropertyType::Enum {
                    values: common_video_resolution_enum_values(false),
                },
                PropertyValue::String(DEFAULT_RESOLUTION.to_string()),
            ),
            block_property(
                "framerate",
                "Max Framerate",
                "Highest rate Chromium paints at. A page that is not changing paints nothing.",
                PropertyType::Enum {
                    values: common_video_framerate_enum_values(false),
                },
                PropertyValue::String(DEFAULT_FRAMERATE.to_string()),
            ),
            block_property(
                stinger::STINGER_SOURCE_PROPERTY,
                "Stinger Source",
                "Use this page as a stinger overlay. A take changes the URL fragment, and the page should run its animation from the start on hashchange, stay fully transparent and still while idle, and end transparent.",
                PropertyType::Bool,
                PropertyValue::Bool(false),
            ),
            block_property(
                stinger::DURATION_PROPERTY,
                "Stinger Duration (ms)",
                "How long the page's stinger animation runs. Required for a stinger source: a page has no length of its own. The keyed input is hidden when it ends.",
                PropertyType::UInt,
                PropertyValue::UInt(0),
            ),
            block_property(
                stinger::CUT_POINT_PROPERTY,
                "Stinger Cut Point (ms)",
                "How far into the animation the program source changes. Set it to the moment the page fully covers the frame. 0 uses the halfway point.",
                PropertyType::UInt,
                PropertyValue::UInt(0),
            ),
            block_property(
                stinger::UNDER_TRANSITION_PROPERTY,
                "Stinger Beneath",
                "Transition running under the page while it covers the frame.",
                PropertyType::Enum {
                    values: stinger::under_transition_enum_values(),
                },
                PropertyValue::String("cut".to_string()),
            ),
            block_property(
                stinger::UNDER_DURATION_PROPERTY,
                "Stinger Beneath Duration (ms)",
                "How long the transition beneath takes. Ignored for a cut, and shortened if it would outlast the animation.",
                PropertyType::UInt,
                PropertyValue::UInt(0),
            ),
        ],
        external_pads: ExternalPads {
            inputs: vec![],
            outputs: vec![ExternalPad {
                label: None,
                name: "video_out".to_string(),
                media_type: MediaType::Video,
                internal_element_id: OUTPUT_ELEMENT.to_string(),
                internal_pad_name: "src".to_string(),
            }],
        },
        built_in: true,
        ui_metadata: Some(BlockUIMetadata {
            icon: Some("🌐".to_string()),
            width: Some(1.5),
            height: Some(2.0),
            ..Default::default()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framerate_parses_as_a_fraction() {
        assert_eq!(parse_fraction("30/1"), Some((30, 1)));
        assert_eq!(parse_fraction("30000/1001"), Some((30000, 1001)));
        assert_eq!(parse_fraction("25"), Some((25, 1)));
        assert_eq!(parse_fraction("0/1"), None);
        assert_eq!(parse_fraction("fast"), None);
    }

    #[test]
    fn resolution_parses_width_by_height() {
        assert_eq!(parse_resolution("1920x1080"), Some((1920, 1080)));
        assert_eq!(parse_resolution(" 640 x 360 "), Some((640, 360)));
        assert_eq!(parse_resolution("0x1080"), None);
        assert_eq!(parse_resolution("1080p"), None);
    }

    #[test]
    fn definition_exposes_the_stinger_properties_the_binding_reads() {
        let def = html_graphic_definition();
        for name in [
            stinger::STINGER_SOURCE_PROPERTY,
            stinger::DURATION_PROPERTY,
            stinger::CUT_POINT_PROPERTY,
            stinger::UNDER_TRANSITION_PROPERTY,
            stinger::UNDER_DURATION_PROPERTY,
        ] {
            assert!(
                def.exposed_properties.iter().any(|p| p.name == name),
                "missing {name}"
            );
        }
        assert_eq!(def.external_pads.outputs[0].name, "video_out");
    }
}
