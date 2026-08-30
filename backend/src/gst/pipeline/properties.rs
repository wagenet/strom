use super::{PipelineError, PipelineManager};
use crate::gst::volume_ramp::VolumeRampManager;
use gstreamer as gst;
use gstreamer::glib;
use gstreamer::prelude::*;
use std::collections::HashMap;
use strom_types::mixer::{DEFAULT_VOLUME_RAMP_MS, MUTE_ANTICLICK_RAMP_MS};
use strom_types::{PipelineState, PropertyValue};
use tracing::{debug, info};

impl PipelineManager {
    /// Set a property on an element.
    ///
    /// `ramp_ms` is consulted only for routes that support smooth interpolation
    /// (currently audio `volume`-element `volume` and `mute`). Other properties
    /// are set immediately regardless. `None` selects the per-route default
    /// ramp (short anti-zipper for `volume`, short anti-click for `mute`); a
    /// caller can request a longer broadcast-style fade by passing an explicit
    /// duration (e.g. 500 ms for a route mute on-air/off-air).
    pub(super) fn set_property(
        &self,
        element: &gst::Element,
        element_id: &str,
        prop_name: &str,
        prop_value: &PropertyValue,
        ramp_ms: Option<u32>,
    ) -> Result<(), PipelineError> {
        debug!(
            "Setting property: {}.{} = {:?} (ramp_ms={:?})",
            element_id, prop_name, prop_value, ramp_ms
        );

        // Audio volume element: route through the ramp manager to avoid
        // zipper noise (volume) and click artifacts (mute). The short-circuit
        // installs a control source / schedules the mute toggle and returns;
        // on failure (e.g. pipeline not yet running) we fall through to the
        // direct `set_property` path below. Volume accepts any numeric
        // PropertyValue (Float/Int/UInt) since clients sometimes send an
        // integer 0/1 — the underlying property is `gdouble`.
        if VolumeRampManager::is_volume_element(element) {
            if prop_name == "volume" {
                let target: Option<f64> = match prop_value {
                    PropertyValue::Float(v) => Some(*v),
                    PropertyValue::Int(v) => Some(*v as f64),
                    PropertyValue::UInt(v) => Some(*v as f64),
                    _ => None,
                };
                if let Some(target) = target {
                    if self.volume_ramps.apply_volume_ramp(
                        element,
                        element_id,
                        target,
                        ramp_ms.unwrap_or(DEFAULT_VOLUME_RAMP_MS),
                    ) {
                        return Ok(());
                    }
                }
            } else if prop_name == "mute" {
                if let PropertyValue::Bool(v) = prop_value {
                    if self.volume_ramps.apply_mute(
                        element,
                        element_id,
                        *v,
                        ramp_ms.unwrap_or(MUTE_ANTICLICK_RAMP_MS),
                    ) {
                        return Ok(());
                    }
                }
            }
        }

        // Everything below ends up in a GLib property setter, and those panic
        // rather than fail: on a property that is missing, read-only or
        // construct-only, on a value whose type GLib would have to coerce, and
        // on one it has to clamp into range. A flow definition names both the
        // property and the value, so all of those are reachable straight from a
        // request body. Resolve the spec first and convert against it.
        let pspec =
            element
                .find_property(prop_name)
                .ok_or_else(|| PipelineError::InvalidProperty {
                    element: element_id.to_string(),
                    property: prop_name.to_string(),
                    reason: "Property not found".to_string(),
                })?;

        let value = checked_property_value(&pspec, prop_value).map_err(|reason| {
            PipelineError::InvalidProperty {
                element: element_id.to_string(),
                property: prop_name.to_string(),
                reason,
            }
        })?;

        set_checked_value(element, prop_name, &value).map_err(|reason| {
            PipelineError::InvalidProperty {
                element: element_id.to_string(),
                property: prop_name.to_string(),
                reason,
            }
        })?;

        Ok(())
    }

    /// Update a property on a live element in the pipeline.
    /// Validates that the property can be changed in the current pipeline state.
    ///
    /// `ramp_ms` is consulted only for routes that support smooth interpolation
    /// (currently audio `volume`-element `volume` and `mute`). For other
    /// properties it is silently ignored.
    pub fn update_element_property(
        &self,
        element_id: &str,
        property_name: &str,
        value: &PropertyValue,
        ramp_ms: Option<u32>,
    ) -> Result<(), PipelineError> {
        debug!(
            "Updating property {}.{} to {:?} on running pipeline (ramp_ms={:?})",
            element_id, property_name, value, ramp_ms
        );

        // Get element reference
        let element = self
            .elements
            .get(element_id)
            .ok_or_else(|| PipelineError::ElementNotFound(element_id.to_string()))?;

        // Get current pipeline state
        let state = self.get_state();

        // Time Offset block: `offset_ms` is not an element property but a
        // pad-offset. Intercept and apply directly, then trigger a latency
        // recalc so downstream sinks resync. The interceptor logs the new
        // value at debug; we don't double-log here.
        if crate::blocks::builtin::time_offset::try_apply_live_offset(
            element,
            element_id,
            property_name,
            value,
        ) {
            let _ = self.pipeline.recalculate_latency();
            return Ok(());
        }

        // Translate property name/value for elements that need conversion.
        // Mixer lsp-rs elements use different property names than LV2 conventions.
        // AudioGain stores gain in dB but GStreamer volume element expects linear.
        let mut translations = crate::blocks::builtin::mixer::translate_property_for_element(
            element,
            property_name,
            value,
        );
        if translations.is_empty() {
            translations = crate::blocks::builtin::audiogain::translate_property(
                element_id,
                property_name,
                value,
            );
        }

        if translations.is_empty() {
            // No translation needed, use original property
            self.validate_property_mutability(element, element_id, property_name, state)?;
            self.set_property(element, element_id, property_name, value, ramp_ms)?;
        } else {
            for (translated_name, translated_value) in &translations {
                debug!(
                    "Translated property {}.{} -> {}.{} for lsp-rs element",
                    element_id, property_name, element_id, translated_name
                );
                self.validate_property_mutability(element, element_id, translated_name, state)?;
                self.set_property(
                    element,
                    element_id,
                    translated_name,
                    translated_value,
                    ramp_ms,
                )?;
            }
        }

        info!(
            "Successfully updated element property {}.{} to {:?}",
            element_id, property_name, value
        );

        Ok(())
    }

    /// Get current value of a property from a live element.
    pub fn get_element_property(
        &self,
        element_id: &str,
        property_name: &str,
    ) -> Result<PropertyValue, PipelineError> {
        let element = self
            .elements
            .get(element_id)
            .ok_or_else(|| PipelineError::ElementNotFound(element_id.to_string()))?;

        // Time Offset block: `offset_ms` is a pad-offset rather than an
        // element property — mirror the write-path interceptor so the value
        // can be read back via the same API.
        if let Some(v) = crate::blocks::builtin::time_offset::try_read_live_offset(
            element,
            element_id,
            property_name,
        ) {
            return Ok(v);
        }

        // Get property spec to determine type
        let pspec =
            element
                .find_property(property_name)
                .ok_or_else(|| PipelineError::InvalidProperty {
                    element: element_id.to_string(),
                    property: property_name.to_string(),
                    reason: "Property not found".to_string(),
                })?;

        let type_name = pspec.value_type().name();

        // Get property value based on type
        let value = match type_name.to_string().as_str() {
            "gchararray" => {
                let v = element.property::<Option<String>>(property_name);
                v.map(PropertyValue::String)
                    .unwrap_or(PropertyValue::String(String::new()))
            }
            "gboolean" => {
                let v = element.property::<bool>(property_name);
                PropertyValue::Bool(v)
            }
            "gint" | "glong" => {
                let v = element.property::<i32>(property_name);
                PropertyValue::Int(v as i64)
            }
            "gint64" => {
                let v = element.property::<i64>(property_name);
                PropertyValue::Int(v)
            }
            "guint" | "gulong" => {
                let v = element.property::<u32>(property_name);
                PropertyValue::UInt(v as u64)
            }
            "guint64" => {
                let v = element.property::<u64>(property_name);
                PropertyValue::UInt(v)
            }
            "gfloat" => {
                let v = element.property::<f32>(property_name);
                PropertyValue::Float(v as f64)
            }
            "gdouble" => {
                let v = element.property::<f64>(property_name);
                PropertyValue::Float(v)
            }
            "GEnum" => {
                // Get enum as string
                // In GStreamer 0.24.x, enum properties have stricter types and can't always be read as i32
                // We need to use the Value API and handle type conversion carefully
                if let Some(param_spec) = pspec.downcast_ref::<glib::ParamSpecEnum>() {
                    let enum_class = param_spec.enum_class();

                    // Get the property as a Value, then try to extract the enum value
                    let value = element.property_value(property_name);

                    // Try to get as i32 (standard enum representation)
                    match value.get::<i32>() {
                        Ok(v) => {
                            if let Some(enum_value) = enum_class.value(v) {
                                PropertyValue::String(enum_value.name().to_string())
                            } else {
                                PropertyValue::Int(v as i64)
                            }
                        }
                        Err(_) => {
                            // Can't convert to i32, this enum type is not supported
                            return Err(PipelineError::InvalidProperty {
                                element: element_id.to_string(),
                                property: property_name.to_string(),
                                reason: format!(
                                    "Cannot read enum property of type {} (not convertible to i32)",
                                    type_name
                                ),
                            });
                        }
                    }
                } else {
                    // Fallback if we can't get the enum class
                    return Err(PipelineError::InvalidProperty {
                        element: element_id.to_string(),
                        property: property_name.to_string(),
                        reason: "Cannot read enum property spec".to_string(),
                    });
                }
            }
            _ => {
                return Err(PipelineError::InvalidProperty {
                    element: element_id.to_string(),
                    property: property_name.to_string(),
                    reason: format!("Unsupported property type: {}", type_name),
                });
            }
        };

        Ok(value)
    }

    /// Get all readable property values from a live element.
    pub fn get_element_properties(
        &self,
        element_id: &str,
    ) -> Result<HashMap<String, PropertyValue>, PipelineError> {
        let element = self
            .elements
            .get(element_id)
            .ok_or_else(|| PipelineError::ElementNotFound(element_id.to_string()))?;

        let mut properties = HashMap::new();

        // Get all properties from the element
        for pspec in element.list_properties() {
            let name = pspec.name().to_string();

            // Skip non-readable properties
            if !pspec.flags().contains(glib::ParamFlags::READABLE) {
                continue;
            }

            // Skip internal/private properties
            if name.starts_with('_') {
                continue;
            }

            // Try to get the property value
            if let Ok(value) = self.get_element_property(element_id, &name) {
                properties.insert(name, value);
            }
        }

        // Surface synthetic block properties that don't live on the element
        // itself (e.g. Time Offset's `offset_ms`, which is a pad-offset).
        if let Some((name, value)) =
            crate::blocks::builtin::time_offset::live_offset_property_entry(element, element_id)
        {
            properties.insert(name, value);
        }

        Ok(properties)
    }

    /// Update a property on a pad in the pipeline.
    /// Validates that the property can be changed in the current pipeline state.
    pub fn update_pad_property(
        &self,
        element_id: &str,
        pad_name: &str,
        property_name: &str,
        value: &PropertyValue,
    ) -> Result<(), PipelineError> {
        debug!(
            "Updating pad property {}:{}:{} to {:?}",
            element_id, pad_name, property_name, value
        );

        // Get element reference
        let element = self
            .elements
            .get(element_id)
            .ok_or_else(|| PipelineError::ElementNotFound(element_id.to_string()))?;

        // Get pad reference - try static pad first, then request pad
        let pad = if let Some(p) = element.static_pad(pad_name) {
            p
        } else if let Some(p) = element.request_pad_simple(pad_name) {
            p
        } else {
            return Err(PipelineError::PadNotFound {
                element: element_id.to_string(),
                pad: pad_name.to_string(),
            });
        };

        // Get current pipeline state
        let state = self.get_state();

        // Validate property is mutable in current state (using pad's property spec)
        self.validate_pad_property_mutability(&pad, element_id, pad_name, property_name, state)?;

        // Set the property on the pad
        self.set_pad_property(&pad, element_id, pad_name, property_name, value)?;

        info!(
            "Successfully updated pad property {}:{}:{} to {:?}",
            element_id, pad_name, property_name, value
        );

        Ok(())
    }

    /// Get current value of a property from a pad.
    pub fn get_pad_property(
        &self,
        element_id: &str,
        pad_name: &str,
        property_name: &str,
    ) -> Result<PropertyValue, PipelineError> {
        let element = self
            .elements
            .get(element_id)
            .ok_or_else(|| PipelineError::ElementNotFound(element_id.to_string()))?;

        // Get pad reference
        let pad = if let Some(p) = element.static_pad(pad_name) {
            p
        } else if let Some(p) = element.request_pad_simple(pad_name) {
            p
        } else {
            return Err(PipelineError::PadNotFound {
                element: element_id.to_string(),
                pad: pad_name.to_string(),
            });
        };

        // Get property spec to determine type
        let pspec =
            pad.find_property(property_name)
                .ok_or_else(|| PipelineError::InvalidProperty {
                    element: format!("{}:{}", element_id, pad_name),
                    property: property_name.to_string(),
                    reason: "Property not found on pad".to_string(),
                })?;

        let type_name = pspec.value_type().name();

        // Get property value based on type
        let value = match type_name.to_string().as_str() {
            "gchararray" => {
                let v = pad.property::<Option<String>>(property_name);
                v.map(PropertyValue::String)
                    .unwrap_or(PropertyValue::String(String::new()))
            }
            "gboolean" => {
                let v = pad.property::<bool>(property_name);
                PropertyValue::Bool(v)
            }
            "gint" | "glong" => {
                let v = pad.property::<i32>(property_name);
                PropertyValue::Int(v as i64)
            }
            "gint64" => {
                let v = pad.property::<i64>(property_name);
                PropertyValue::Int(v)
            }
            "guint" | "gulong" => {
                let v = pad.property::<u32>(property_name);
                PropertyValue::UInt(v as u64)
            }
            "guint64" => {
                let v = pad.property::<u64>(property_name);
                PropertyValue::UInt(v)
            }
            "gfloat" => {
                let v = pad.property::<f32>(property_name);
                PropertyValue::Float(v as f64)
            }
            "gdouble" => {
                let v = pad.property::<f64>(property_name);
                PropertyValue::Float(v)
            }
            _ => {
                // Check if it's an enum type
                if pspec.value_type().is_a(glib::Type::ENUM) {
                    // Get the enum value as an integer and convert to nick string
                    let value = pad.property_value(property_name);
                    if let Ok(enum_value) = value.get::<i32>() {
                        // Get the enum class and find the nick for this value
                        if let Some(enum_class) = glib::EnumClass::with_type(pspec.value_type()) {
                            if let Some(enum_val) = enum_class.value(enum_value) {
                                PropertyValue::String(enum_val.nick().to_string())
                            } else {
                                PropertyValue::Int(enum_value as i64)
                            }
                        } else {
                            PropertyValue::Int(enum_value as i64)
                        }
                    } else {
                        return Err(PipelineError::InvalidProperty {
                            element: format!("{}:{}", element_id, pad_name),
                            property: property_name.to_string(),
                            reason: format!("Failed to read enum value for type: {}", type_name),
                        });
                    }
                } else {
                    return Err(PipelineError::InvalidProperty {
                        element: format!("{}:{}", element_id, pad_name),
                        property: property_name.to_string(),
                        reason: format!("Unsupported property type: {}", type_name),
                    });
                }
            }
        };

        Ok(value)
    }

    /// Get all readable property values from a pad.
    pub fn get_pad_properties(
        &self,
        element_id: &str,
        pad_name: &str,
    ) -> Result<HashMap<String, PropertyValue>, PipelineError> {
        let element = self
            .elements
            .get(element_id)
            .ok_or_else(|| PipelineError::ElementNotFound(element_id.to_string()))?;

        // Get pad reference
        let pad = if let Some(p) = element.static_pad(pad_name) {
            p
        } else if let Some(p) = element.request_pad_simple(pad_name) {
            p
        } else {
            return Err(PipelineError::PadNotFound {
                element: element_id.to_string(),
                pad: pad_name.to_string(),
            });
        };

        let mut properties = HashMap::new();

        // Get all properties from the pad
        for pspec in pad.list_properties() {
            let name = pspec.name().to_string();

            // Skip non-readable properties
            if !pspec.flags().contains(glib::ParamFlags::READABLE) {
                continue;
            }

            // Skip internal/private properties
            if name.starts_with('_') {
                continue;
            }

            // Try to get the property value
            if let Ok(value) = self.get_pad_property(element_id, pad_name, &name) {
                properties.insert(name, value);
            }
        }

        Ok(properties)
    }

    /// Set a property on a pad.
    pub(super) fn set_pad_property(
        &self,
        pad: &gst::Pad,
        element_id: &str,
        pad_name: &str,
        prop_name: &str,
        prop_value: &PropertyValue,
    ) -> Result<(), PipelineError> {
        debug!(
            "Setting pad property: {}:{}:{} = {:?}",
            element_id, pad_name, prop_name, prop_value
        );

        // Some pad properties are version- or backend-specific. The clearest
        // example is `sizing-policy`, which exists on `GstVideoAggregatorPad`
        // and `GstGLVideoMixerPad` only from GStreamer 1.24 onward. On 1.22
        // calling `set_property_from_str` for a missing property panics
        // ("property '...' of type '...' not found"), which would crash the
        // tokio worker building the pipeline. Skip gracefully instead.
        if !pad.has_property(prop_name) {
            debug!(
                "Pad {}:{} has no property '{}' (likely a GStreamer version/backend difference) - skipping",
                element_id, pad_name, prop_name
            );
            return Ok(());
        }

        // Same checked conversion as the element path: `set_property_from_str`
        // panics on a value it cannot parse for the property, and pad
        // properties come from the flow definition too.
        let pspec = pad
            .find_property(prop_name)
            .ok_or_else(|| PipelineError::InvalidProperty {
                element: format!("{}:{}", element_id, pad_name),
                property: prop_name.to_string(),
                reason: "Property not found on pad".to_string(),
            })?;

        let value = checked_property_value(&pspec, prop_value).map_err(|reason| {
            PipelineError::InvalidProperty {
                element: format!("{}:{}", element_id, pad_name),
                property: prop_name.to_string(),
                reason,
            }
        })?;

        set_checked_value(pad, prop_name, &value).map_err(|reason| {
            PipelineError::InvalidProperty {
                element: format!("{}:{}", element_id, pad_name),
                property: prop_name.to_string(),
                reason,
            }
        })?;

        Ok(())
    }

    /// Validate that a pad property can be changed in the current pipeline state.
    fn validate_pad_property_mutability(
        &self,
        pad: &gst::Pad,
        element_id: &str,
        pad_name: &str,
        property_name: &str,
        current_state: PipelineState,
    ) -> Result<(), PipelineError> {
        let pspec =
            pad.find_property(property_name)
                .ok_or_else(|| PipelineError::InvalidProperty {
                    element: format!("{}:{}", element_id, pad_name),
                    property: property_name.to_string(),
                    reason: "Property not found on pad".to_string(),
                })?;

        let flags = pspec.flags();

        // Check if property is writable
        if !flags.contains(glib::ParamFlags::WRITABLE) {
            return Err(PipelineError::InvalidProperty {
                element: format!("{}:{}", element_id, pad_name),
                property: property_name.to_string(),
                reason: "Property is not writable".to_string(),
            });
        }

        // Check if property is construct-only
        if flags.contains(glib::ParamFlags::CONSTRUCT_ONLY) {
            return Err(PipelineError::InvalidProperty {
                element: format!("{}:{}", element_id, pad_name),
                property: property_name.to_string(),
                reason: "Property is construct-only and cannot be changed after pad creation"
                    .to_string(),
            });
        }

        // Check if property can be changed in current state
        // GStreamer-specific flags (from gstreamer-sys)
        // GST_PARAM_MUTABLE_READY = 0x400
        // GST_PARAM_MUTABLE_PAUSED = 0x800
        // GST_PARAM_MUTABLE_PLAYING = 0x1000
        // GST_PARAM_CONTROLLABLE = 0x200
        let flags_bits = flags.bits();
        let mutable_in_ready = (flags_bits & 0x400) != 0;
        let mutable_in_paused = (flags_bits & 0x800) != 0;
        let mutable_in_playing = (flags_bits & 0x1000) != 0;
        let controllable = (flags_bits & 0x200) != 0;

        // Controllable properties can generally be changed at runtime
        let can_change_at_runtime = controllable;

        match current_state {
            PipelineState::Playing => {
                if !mutable_in_playing && !can_change_at_runtime {
                    return Err(PipelineError::PropertyNotMutable {
                        element: format!("{}:{}", element_id, pad_name),
                        property: property_name.to_string(),
                        state: current_state,
                    });
                }
            }
            PipelineState::Paused => {
                if !mutable_in_paused && !mutable_in_playing && !can_change_at_runtime {
                    return Err(PipelineError::PropertyNotMutable {
                        element: format!("{}:{}", element_id, pad_name),
                        property: property_name.to_string(),
                        state: current_state,
                    });
                }
            }
            PipelineState::Ready => {
                if !mutable_in_ready && !mutable_in_paused && !mutable_in_playing {
                    return Err(PipelineError::PropertyNotMutable {
                        element: format!("{}:{}", element_id, pad_name),
                        property: property_name.to_string(),
                        state: current_state,
                    });
                }
            }
            PipelineState::Null => {
                // All writable, non-construct-only properties can be changed in NULL state
            }
        }

        Ok(())
    }

    /// Validate that a property can be changed in the current pipeline state.
    fn validate_property_mutability(
        &self,
        element: &gst::Element,
        element_id: &str,
        property_name: &str,
        current_state: PipelineState,
    ) -> Result<(), PipelineError> {
        let pspec =
            element
                .find_property(property_name)
                .ok_or_else(|| PipelineError::InvalidProperty {
                    element: element_id.to_string(),
                    property: property_name.to_string(),
                    reason: "Property not found".to_string(),
                })?;

        let flags = pspec.flags();

        // Check if property is writable
        if !flags.contains(glib::ParamFlags::WRITABLE) {
            return Err(PipelineError::InvalidProperty {
                element: element_id.to_string(),
                property: property_name.to_string(),
                reason: "Property is not writable".to_string(),
            });
        }

        // Check if property is construct-only
        if flags.contains(glib::ParamFlags::CONSTRUCT_ONLY) {
            return Err(PipelineError::InvalidProperty {
                element: element_id.to_string(),
                property: property_name.to_string(),
                reason: "Property is construct-only and cannot be changed after element creation"
                    .to_string(),
            });
        }

        // Check if property can be changed in current state
        // GStreamer-specific flags (from gstreamer-sys)
        // GST_PARAM_MUTABLE_READY = 0x400
        // GST_PARAM_MUTABLE_PAUSED = 0x800
        // GST_PARAM_MUTABLE_PLAYING = 0x1000
        // GST_PARAM_CONTROLLABLE = 0x200
        let flags_bits = flags.bits();
        let mutable_in_ready = (flags_bits & 0x400) != 0;
        let mutable_in_paused = (flags_bits & 0x800) != 0;
        let mutable_in_playing = (flags_bits & 0x1000) != 0;
        let controllable = (flags_bits & 0x200) != 0;

        // Controllable properties can generally be changed at runtime
        // They're designed for dynamic updates via GstController
        let can_change_at_runtime = controllable;

        match current_state {
            PipelineState::Playing => {
                if !mutable_in_playing && !can_change_at_runtime {
                    return Err(PipelineError::PropertyNotMutable {
                        element: element_id.to_string(),
                        property: property_name.to_string(),
                        state: current_state,
                    });
                }
            }
            PipelineState::Paused => {
                if !mutable_in_paused && !mutable_in_playing && !can_change_at_runtime {
                    return Err(PipelineError::PropertyNotMutable {
                        element: element_id.to_string(),
                        property: property_name.to_string(),
                        state: current_state,
                    });
                }
            }
            PipelineState::Ready => {
                if !mutable_in_ready && !mutable_in_paused && !mutable_in_playing {
                    return Err(PipelineError::PropertyNotMutable {
                        element: element_id.to_string(),
                        property: property_name.to_string(),
                        state: current_state,
                    });
                }
            }
            PipelineState::Null => {
                // All writable, non-construct-only properties can be changed in NULL state
            }
        }

        Ok(())
    }
}

/// How many enum members an error message lists before it gives up.
const MAX_LISTED_ENUM_VALUES: usize = 24;

/// Convert a client-supplied `PropertyValue` into a `glib::Value` that `pspec`
/// is known to accept.
///
/// GLib's property setters signal every kind of misuse by panicking: an
/// unwritable or construct-only property, a value whose type it would have to
/// coerce, and a value `g_param_value_validate` has to clamp all abort the
/// calling thread. `POST /api/flows` lets a client pick the property name and
/// the value, so an unchecked `set_property` turns a request body into a panic
/// — `{"pattern": 1}` on a `videotestsrc` used to unwind the task serving the
/// request, taking the flow's teardown with it. Nothing reaches GLib from here
/// without having been checked against the spec first.
fn checked_property_value(
    pspec: &glib::ParamSpec,
    prop_value: &PropertyValue,
) -> Result<glib::Value, String> {
    if !pspec.flags().contains(glib::ParamFlags::WRITABLE) {
        return Err("Property is not writable".to_string());
    }
    if pspec.flags().contains(glib::ParamFlags::CONSTRUCT_ONLY) {
        return Err(
            "Property is construct-only and cannot be set after element creation".to_string(),
        );
    }

    let target = pspec.value_type();
    let target_name = target.name();

    // Strings go through GStreamer's own deserializer, which understands enum
    // nicks, flag lists ("a+b"), caps, fractions and structures. It is the same
    // conversion `set_property_from_str` performs, without its `unwrap` — a
    // string that doesn't parse comes back as an error rather than a panic.
    if let PropertyValue::String(v) = prop_value {
        if target == gst::Structure::static_type() && v == "NULL" {
            return Ok(None::<gst::Structure>.to_value());
        }
        return glib::Value::deserialize_with_pspec(v, pspec)
            .map_err(|_| format!("Cannot parse \"{}\" as {}", v, target_name));
    }

    if let PropertyValue::Bool(v) = prop_value {
        return if target == glib::Type::BOOL {
            Ok(v.to_value())
        } else {
            Err(format!("Property expects {}, got a boolean", target_name))
        };
    }

    // GLib enums are integers underneath, so an integer that names a real
    // member is as good as its nick string. One that doesn't is an error:
    // GLib would clamp it to the property default and then panic because it
    // had to clamp.
    if target.is_a(glib::Type::ENUM) {
        let n = integer_operand(prop_value, target_name)?;
        let class = glib::EnumClass::with_type(target)
            .ok_or_else(|| format!("Cannot read the enum class of {}", target_name))?;
        return i32::try_from(n)
            .ok()
            .and_then(|n| class.to_value(n))
            .ok_or_else(|| {
                format!(
                    "{} is not a valid {} value ({})",
                    n,
                    target_name,
                    enum_choices(&class)
                )
            });
    }

    // Flags are a bitmask of integers, so the same applies bit by bit. The
    // deserializer takes the whole mask at once but does not check it against
    // the class, so check it here and let the deserializer do the assembling.
    if target.is_a(glib::Type::FLAGS) {
        let n = integer_operand(prop_value, target_name)?;
        let class = glib::FlagsClass::with_type(target)
            .ok_or_else(|| format!("Cannot read the flags class of {}", target_name))?;
        let mask = class.values().iter().fold(0u32, |m, v| m | v.value());
        let bits = u32::try_from(n)
            .ok()
            .filter(|b| b & !mask == 0)
            .ok_or_else(|| {
                format!(
                    "{} sets bits that are not part of {} (valid mask: {:#x})",
                    n, target_name, mask
                )
            })?;
        return glib::Value::deserialize_with_pspec(&bits.to_string(), pspec)
            .map_err(|_| format!("Cannot set {} to {}", target_name, bits));
    }

    // Numbers: convert into the property's own type and reject anything outside
    // the range the spec declares, for the same reason as the enum above.
    macro_rules! ranged_int {
        ($spec:ty, $rust:ty) => {{
            let n = integer_operand(prop_value, target_name)?;
            let (min, max) = pspec
                .downcast_ref::<$spec>()
                .map(|p| (p.minimum() as i128, p.maximum() as i128))
                .unwrap_or((<$rust>::MIN as i128, <$rust>::MAX as i128));
            if n < min || n > max {
                return Err(format!(
                    "Value {} is out of range for {} ({}..={})",
                    n, target_name, min, max
                ));
            }
            Ok((n as $rust).to_value())
        }};
    }

    macro_rules! ranged_float {
        ($spec:ty, $rust:ty) => {{
            let n = float_operand(prop_value, target_name)?;
            let (min, max) = pspec
                .downcast_ref::<$spec>()
                .map(|p| (p.minimum() as f64, p.maximum() as f64))
                .unwrap_or((<$rust>::MIN as f64, <$rust>::MAX as f64));
            // A NaN fails this comparison too, which is what we want.
            if !(min..=max).contains(&n) {
                return Err(format!(
                    "Value {} is out of range for {} ({}..={})",
                    n, target_name, min, max
                ));
            }
            Ok((n as $rust).to_value())
        }};
    }

    match target {
        glib::Type::I8 => ranged_int!(glib::ParamSpecChar, i8),
        glib::Type::U8 => ranged_int!(glib::ParamSpecUChar, u8),
        glib::Type::I32 => ranged_int!(glib::ParamSpecInt, i32),
        glib::Type::U32 => ranged_int!(glib::ParamSpecUInt, u32),
        glib::Type::I64 => ranged_int!(glib::ParamSpecInt64, i64),
        glib::Type::U64 => ranged_int!(glib::ParamSpecUInt64, u64),
        glib::Type::F32 => ranged_float!(glib::ParamSpecFloat, f32),
        glib::Type::F64 => ranged_float!(glib::ParamSpecDouble, f64),
        // Everything else (caps, structures, fractions, boxed and object types,
        // and the C `long` types glib-rs cannot build a `Value` for) is only
        // reachable through the string form above.
        _ => Err(format!(
            "Property of type {} cannot be set from {}; send it as a string",
            target_name,
            describe(prop_value)
        )),
    }
}

/// Apply an already-checked value, refusing to let GLib abort the thread.
///
/// `checked_property_value` aims to leave nothing for GLib to reject, but a
/// `GParamSpec` subclass may carry validation of its own that it can only
/// report by panicking. An unwind from here would cross the axum handler,
/// which drops the client's connection without a response and skips the
/// caller's teardown, leaving a half-built flow behind — so treat a panic as
/// one more way for the value to be invalid.
fn set_checked_value<O: IsA<glib::Object>>(
    obj: &O,
    prop_name: &str,
    value: &glib::Value,
) -> Result<(), String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        obj.set_property_from_value(prop_name, value);
    }))
    .map_err(|_| "GLib rejected the value (see the log for details)".to_string())
}

/// The integer a client meant, whatever numeric shape it arrived in.
fn integer_operand(prop_value: &PropertyValue, target_name: &str) -> Result<i128, String> {
    match prop_value {
        PropertyValue::Int(v) => Ok(*v as i128),
        PropertyValue::UInt(v) => Ok(*v as i128),
        // A client that went through JSON may have lost the difference between
        // 2 and 2.0. A value with an actual fraction is a real mismatch.
        PropertyValue::Float(v) if v.is_finite() && v.fract() == 0.0 => Ok(*v as i128),
        PropertyValue::Float(v) => Err(format!(
            "Property expects {}, got the non-integer value {}",
            target_name, v
        )),
        PropertyValue::Bool(_) => Err(format!("Property expects {}, got a boolean", target_name)),
        PropertyValue::String(_) => Err(format!("Property expects {}, got a string", target_name)),
    }
}

/// The float a client meant, whatever numeric shape it arrived in.
fn float_operand(prop_value: &PropertyValue, target_name: &str) -> Result<f64, String> {
    match prop_value {
        PropertyValue::Float(v) => Ok(*v),
        PropertyValue::Int(v) => Ok(*v as f64),
        PropertyValue::UInt(v) => Ok(*v as f64),
        PropertyValue::Bool(_) => Err(format!("Property expects {}, got a boolean", target_name)),
        PropertyValue::String(_) => Err(format!("Property expects {}, got a string", target_name)),
    }
}

/// The members of an enum, for an error message that tells a client what it
/// could have sent instead.
fn enum_choices(class: &glib::EnumClass) -> String {
    let values = class.values();
    let mut listed: Vec<String> = values
        .iter()
        .take(MAX_LISTED_ENUM_VALUES)
        .map(|v| format!("{}={}", v.value(), v.nick()))
        .collect();
    if values.len() > MAX_LISTED_ENUM_VALUES {
        listed.push("...".to_string());
    }
    format!("valid: {}", listed.join(", "))
}

/// What the client sent, named the way the client would recognise it.
fn describe(prop_value: &PropertyValue) -> &'static str {
    match prop_value {
        PropertyValue::String(_) => "a string",
        PropertyValue::Int(_) => "an integer",
        PropertyValue::UInt(_) => "an integer",
        PropertyValue::Float(_) => "a number",
        PropertyValue::Bool(_) => "a boolean",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `videotestsrc` carries one of everything this module has to get right:
    /// an enum (`pattern`), a ranged integer (`num-buffers`), a boolean
    /// (`is-live`) and, from `GstObject`, a string (`name`). It ships in
    /// gstreamer-plugins-base, which CI installs, so these tests run rather
    /// than skip.
    fn videotestsrc() -> gst::Element {
        gst::init().unwrap();
        gst::ElementFactory::make("videotestsrc")
            .build()
            .expect("videotestsrc missing - install gstreamer1.0-plugins-base")
    }

    fn convert(
        element: &gst::Element,
        prop_name: &str,
        prop_value: PropertyValue,
    ) -> Result<glib::Value, String> {
        let pspec = element
            .find_property(prop_name)
            .unwrap_or_else(|| panic!("videotestsrc has no '{}' property", prop_name));
        checked_property_value(&pspec, &prop_value)
    }

    /// An integer names an enum member as well as its nick does, and the value
    /// that comes back is the enum's own type — not the `gint64` that used to
    /// be handed to `set_property`.
    #[test]
    fn enum_accepts_an_in_range_integer() {
        let src = videotestsrc();
        let value = convert(&src, "pattern", PropertyValue::Int(1)).expect("1 is `snow`");

        let (_class, member) =
            glib::EnumValue::from_value(&value).expect("not an enum value of the property's type");
        assert_eq!(member.nick(), "snow");

        // And it is actually accepted by the element.
        src.set_property_from_value("pattern", &value);
        assert_eq!(
            glib::EnumValue::from_value(&src.property_value("pattern"))
                .unwrap()
                .1
                .nick(),
            "snow"
        );
    }

    #[test]
    fn enum_accepts_a_nick_string() {
        let src = videotestsrc();
        let value = convert(&src, "pattern", PropertyValue::String("snow".to_string()))
            .expect("`snow` is a pattern nick");
        assert_eq!(
            glib::EnumValue::from_value(&value).unwrap().1.value(),
            1,
            "nick did not resolve to the same member as the integer"
        );
    }

    /// The reported panic: GLib clamps an unknown enum integer to the property
    /// default and then aborts because it had to clamp.
    #[test]
    fn enum_rejects_an_out_of_range_integer() {
        let src = videotestsrc();
        let err = convert(&src, "pattern", PropertyValue::Int(99_999))
            .expect_err("99999 is not a GstVideoTestSrcPattern");
        assert!(err.contains("99999"), "unhelpful message: {}", err);
        assert!(
            err.contains("snow"),
            "message lists no alternatives: {}",
            err
        );
    }

    #[test]
    fn enum_rejects_an_unknown_nick() {
        let src = videotestsrc();
        convert(
            &src,
            "pattern",
            PropertyValue::String("harlequin".to_string()),
        )
        .expect_err("`harlequin` is not a pattern nick");
    }

    #[test]
    fn integer_property_accepts_a_value_inside_the_declared_range() {
        let src = videotestsrc();
        let value = convert(&src, "num-buffers", PropertyValue::Int(10)).expect("10 is in range");
        assert_eq!(value.get::<i32>().expect("not a gint"), 10);
    }

    /// `num-buffers` is a `gint` but its spec starts at -1. A value GLib would
    /// have to clamp panics just like a type mismatch does.
    #[test]
    fn integer_property_rejects_a_value_the_spec_would_clamp() {
        let src = videotestsrc();
        let err = convert(&src, "num-buffers", PropertyValue::Int(-5))
            .expect_err("-5 is below the num-buffers minimum");
        assert!(err.contains("-1"), "message omits the range: {}", err);
    }

    #[test]
    fn integer_property_rejects_a_value_too_wide_for_the_type() {
        let src = videotestsrc();
        convert(&src, "num-buffers", PropertyValue::Int(5_000_000_000))
            .expect_err("5000000000 does not fit in a gint");
    }

    /// A client that has been through JSON may have lost the difference
    /// between 2 and 2.0, but 2.5 is a real mismatch.
    #[test]
    fn integer_property_accepts_a_whole_float_but_not_a_fractional_one() {
        let src = videotestsrc();
        let value =
            convert(&src, "num-buffers", PropertyValue::Float(10.0)).expect("10.0 is a whole 10");
        assert_eq!(value.get::<i32>().unwrap(), 10);

        convert(&src, "num-buffers", PropertyValue::Float(10.5))
            .expect_err("10.5 is not an integer");
    }

    #[test]
    fn integer_property_rejects_a_string_that_is_not_a_number() {
        let src = videotestsrc();
        convert(
            &src,
            "num-buffers",
            PropertyValue::String("not-a-number".to_string()),
        )
        .expect_err("`not-a-number` is not a gint");
    }

    #[test]
    fn boolean_property_rejects_a_non_boolean_string() {
        let src = videotestsrc();
        convert(&src, "is-live", PropertyValue::String("banana".to_string()))
            .expect_err("`banana` is not a gboolean");
    }

    #[test]
    fn string_property_rejects_a_number() {
        let src = videotestsrc();
        let err = convert(&src, "name", PropertyValue::Int(5))
            .expect_err("an integer is not a gchararray");
        assert!(err.contains("integer"), "unhelpful message: {}", err);
    }

    /// The `gdouble` coercion the mixer relies on: a client that sends volume
    /// as 1 rather than 1.0 still gets a gdouble.
    #[test]
    fn double_property_accepts_an_integer() {
        gst::init().unwrap();
        let volume = gst::ElementFactory::make("volume")
            .build()
            .expect("volume missing - install gstreamer1.0-plugins-base");
        let pspec = volume.find_property("volume").unwrap();
        let value = checked_property_value(&pspec, &PropertyValue::Int(1)).expect("1 is a volume");
        assert_eq!(value.get::<f64>().expect("not a gdouble"), 1.0);
    }
}
