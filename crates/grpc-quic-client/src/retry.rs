//! Retry policy with exponential backoff.

use std::time::Duration;

/// Configuration for retry behaviour on transient failures.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the initial one).
    pub max_attempts: u32,
    /// Initial backoff duration before the first retry.
    pub initial_backoff: Duration,
    /// Multiplier applied to the backoff after each failure.
    pub backoff_multiplier: f64,
    /// Maximum backoff duration cap.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            backoff_multiplier: 2.0,
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Compute the backoff duration for the given attempt index (0-based).
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        let factor = self.backoff_multiplier.powi(attempt as i32);
        let millis = (self.initial_backoff.as_millis() as f64 * factor) as u64;
        Duration::from_millis(millis).min(self.max_backoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially() {
        let policy = RetryPolicy::default();
        let b0 = policy.backoff_for(0);
        let b1 = policy.backoff_for(1);
        let b2 = policy.backoff_for(2);
        assert!(b1 > b0, "backoff should grow");
        assert!(b2 > b1, "backoff should grow");
        assert!(b2 <= policy.max_backoff, "backoff must not exceed cap");
    }

    #[test]
    fn backoff_capped_at_max() {
        let policy = RetryPolicy {
            max_backoff: Duration::from_millis(500),
            ..Default::default()
        };
        let large = policy.backoff_for(100);
        assert_eq!(large, Duration::from_millis(500));
    }
}
