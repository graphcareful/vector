use vector_common::finalization::{EventFinalizerGroups, EventStatus};

/// Resolves finalizers as errored on drop unless ownership is explicitly transferred or the
/// associated record is intentionally dropped.
pub(crate) struct FinalizerGuard {
    finalizers: EventFinalizerGroups,
    error_on_drop: bool,
}

impl FinalizerGuard {
    pub(crate) fn new(finalizers: EventFinalizerGroups) -> Self {
        Self {
            finalizers,
            error_on_drop: true,
        }
    }

    /// Releases the guard without marking the finalizers as errored.
    pub(crate) fn disarm(mut self) {
        self.error_on_drop = false;
    }

    /// Transfers ownership of the finalizers without changing their status.
    pub(crate) fn into_inner(mut self) -> EventFinalizerGroups {
        self.error_on_drop = false;
        std::mem::take(&mut self.finalizers)
    }
}

impl Drop for FinalizerGuard {
    fn drop(&mut self) {
        if self.error_on_drop {
            self.finalizers.update_status(EventStatus::Errored);
        }
    }
}
