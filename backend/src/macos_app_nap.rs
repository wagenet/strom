//! Keep macOS from putting the server into the background QoS band.
//!
//! Headless Strom is a Cocoa process that never opens a window: `gst_macos_main`
//! runs a CFRunLoop on the main thread so `cefsrc` can initialise. macOS treats a
//! windowless app as idle-by-default and, about thirty seconds after launch, moves
//! the whole task into the background QoS band — every thread drops to scheduling
//! priority 4, which on Apple Silicon means efficiency cores and a throttled clock.
//! Measured on an M-series laptop, the same 1080p x264 encode takes 4.5 s in a
//! process younger than that and 25-30 s in one older, whether or not the process
//! did any work in between. A restart appears to "fix" it, for another thirty
//! seconds.
//!
//! `beginActivityWithOptions:reason:` is the documented opt-out. Holding a
//! user-initiated activity for the life of the process tells macOS the work is on
//! behalf of the user, so the task stays in the normal band.
//!
//! Only headless mode needs this. In GUI mode the window itself keeps the app out
//! of App Nap.

/// Take a user-initiated activity assertion that lasts as long as the process.
///
/// Safe to call more than once; each call takes its own assertion.
#[cfg(target_os = "macos")]
pub fn hold_activity_for_process_lifetime() {
    use objc2_foundation::{NSActivityOptions, NSProcessInfo, NSString};

    // `UserInitiatedAllowingIdleSystemSleep` rather than `UserInitiated`: both
    // opt out of App Nap, but the plain variant also blocks system idle sleep,
    // and a media server has no business keeping the machine awake.
    // `LatencyCritical` additionally opts out of timer coalescing, which is what
    // the streaming threads want.
    let options = NSActivityOptions::UserInitiatedAllowingIdleSystemSleep
        | NSActivityOptions::LatencyCritical;
    let reason = NSString::from_str("Strom is running media pipelines");
    let activity = NSProcessInfo::processInfo().beginActivityWithOptions_reason(options, &reason);

    // The assertion lasts exactly as long as the token is alive, and we want it
    // for the whole process. Leaking it is the intent, not an oversight: there is
    // no point in the server's life at which we would want to be App-Napped.
    std::mem::forget(activity);

    tracing::info!("macOS: holding a user-initiated activity assertion (App Nap opt-out)");
}

/// No-op away from macOS.
#[cfg(not(target_os = "macos"))]
pub fn hold_activity_for_process_lifetime() {}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    /// Cheap companion to `tests/macos_app_nap_test.rs`. That one drives the
    /// real binary and is the actual guard, but costs ~50 s and is opt-in; this
    /// runs on every `cargo test` and catches a wrong selector or option
    /// constant straight away, which aborts the process rather than failing an
    /// assertion.
    #[test]
    fn taking_the_activity_assertion_does_not_abort() {
        super::hold_activity_for_process_lifetime();
        super::hold_activity_for_process_lifetime();
    }
}
