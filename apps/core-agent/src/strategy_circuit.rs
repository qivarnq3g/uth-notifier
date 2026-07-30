use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Serialize;
use uth_storage::CrawlStrategyHealthSample;

#[derive(Debug, Clone, Copy)]
pub struct StrategyCircuitPolicy {
    pub failure_threshold: u32,
    pub cooldown: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategySelection {
    pub enabled: Vec<String>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyCircuitSnapshot {
    pub strategy: String,
    pub state: &'static str,
    pub consecutive_failures: u32,
    pub cooldown_remaining_seconds: u64,
}

#[derive(Debug, Default)]
struct StrategyCircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    probe_in_flight: bool,
}

#[derive(Debug)]
pub struct StrategyCircuitBreaker {
    policy: StrategyCircuitPolicy,
    states: BTreeMap<String, StrategyCircuitState>,
}

impl StrategyCircuitBreaker {
    pub fn new(
        strategies: &[String],
        history: &[CrawlStrategyHealthSample],
        policy: StrategyCircuitPolicy,
        now: Instant,
    ) -> Self {
        let history = history
            .iter()
            .map(|sample| (sample.strategy.as_str(), sample))
            .collect::<BTreeMap<_, _>>();
        let states = strategies
            .iter()
            .map(|strategy| {
                let state = history
                    .get(strategy.as_str())
                    .filter(|sample| {
                        sample.attempts >= u64::from(policy.failure_threshold)
                            && sample.healthy == 0
                    })
                    .map(|_| StrategyCircuitState {
                        consecutive_failures: policy.failure_threshold,
                        open_until: now.checked_add(policy.cooldown),
                        probe_in_flight: false,
                    })
                    .unwrap_or_default();
                (strategy.clone(), state)
            })
            .collect();
        Self { policy, states }
    }

    pub fn select(&mut self, strategies: &[String], now: Instant) -> StrategySelection {
        let mut enabled = Vec::with_capacity(strategies.len());
        let mut skipped = Vec::new();
        for strategy in strategies {
            let state = self.states.entry(strategy.clone()).or_default();
            match state.open_until {
                Some(open_until) if now < open_until => skipped.push(strategy.clone()),
                Some(_) if state.probe_in_flight => skipped.push(strategy.clone()),
                Some(_) => {
                    state.probe_in_flight = true;
                    enabled.push(strategy.clone());
                }
                None => enabled.push(strategy.clone()),
            }
        }
        StrategySelection { enabled, skipped }
    }

    pub fn observe(
        &mut self,
        selected: &[String],
        outcomes: &BTreeMap<String, String>,
        crawl_failed: bool,
        now: Instant,
    ) {
        for strategy in selected {
            let state = self.states.entry(strategy.clone()).or_default();
            let was_probe = state.probe_in_flight;
            state.probe_in_flight = false;
            match outcomes.get(strategy) {
                Some(outcome) if outcome == "healthy" => {
                    state.consecutive_failures = 0;
                    state.open_until = None;
                }
                Some(_) => self.record_failure(strategy, was_probe, now),
                None if crawl_failed => self.record_failure(strategy, was_probe, now),
                None => {}
            }
        }
    }

    pub fn snapshots(&self, now: Instant) -> Vec<StrategyCircuitSnapshot> {
        self.states
            .iter()
            .map(|(strategy, state)| {
                let (circuit_state, cooldown_remaining_seconds) = if state.probe_in_flight {
                    ("half_open", 0)
                } else if let Some(open_until) =
                    state.open_until.filter(|open_until| *open_until > now)
                {
                    ("open", open_until.saturating_duration_since(now).as_secs())
                } else {
                    ("closed", 0)
                };
                StrategyCircuitSnapshot {
                    strategy: strategy.clone(),
                    state: circuit_state,
                    consecutive_failures: state.consecutive_failures,
                    cooldown_remaining_seconds,
                }
            })
            .collect()
    }

    fn record_failure(&mut self, strategy: &str, was_probe: bool, now: Instant) {
        let state = self.states.entry(strategy.to_owned()).or_default();
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if was_probe || state.consecutive_failures >= self.policy.failure_threshold {
            state.open_until = now.checked_add(self.policy.cooldown);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use uth_storage::CrawlStrategyHealthSample;

    use super::{StrategyCircuitBreaker, StrategyCircuitPolicy};

    fn strategies() -> Vec<String> {
        ["standard", "polite"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    fn policy() -> StrategyCircuitPolicy {
        StrategyCircuitPolicy {
            failure_threshold: 3,
            cooldown: Duration::from_secs(60),
        }
    }

    #[test]
    fn history_opens_only_a_strategy_without_recent_success() {
        let now = Instant::now();
        let history = vec![
            CrawlStrategyHealthSample {
                strategy: "standard".to_owned(),
                attempts: 3,
                healthy: 0,
            },
            CrawlStrategyHealthSample {
                strategy: "polite".to_owned(),
                attempts: 3,
                healthy: 1,
            },
        ];
        let mut circuit = StrategyCircuitBreaker::new(&strategies(), &history, policy(), now);

        let selection = circuit.select(&strategies(), now);

        assert_eq!(selection.enabled, vec!["polite"]);
        assert_eq!(selection.skipped, vec!["standard"]);
    }

    #[test]
    fn cooldown_allows_one_probe_and_reopens_after_failure() {
        let now = Instant::now();
        let history = vec![CrawlStrategyHealthSample {
            strategy: "standard".to_owned(),
            attempts: 3,
            healthy: 0,
        }];
        let configured = vec!["standard".to_owned()];
        let mut circuit = StrategyCircuitBreaker::new(&configured, &history, policy(), now);
        let probe_at = now + Duration::from_secs(61);

        let probe = circuit.select(&configured, probe_at);
        let concurrent = circuit.select(&configured, probe_at);
        assert_eq!(probe.enabled, configured);
        assert_eq!(concurrent.skipped, configured);

        circuit.observe(
            &probe.enabled,
            &BTreeMap::from([("standard".to_owned(), "http_error".to_owned())]),
            false,
            probe_at,
        );

        assert_eq!(
            circuit
                .select(&configured, probe_at + Duration::from_secs(1))
                .skipped,
            configured
        );
    }

    #[test]
    fn successful_probe_closes_the_circuit() {
        let now = Instant::now();
        let history = vec![CrawlStrategyHealthSample {
            strategy: "standard".to_owned(),
            attempts: 3,
            healthy: 0,
        }];
        let configured = vec!["standard".to_owned()];
        let mut circuit = StrategyCircuitBreaker::new(&configured, &history, policy(), now);
        let probe_at = now + Duration::from_secs(61);
        let probe = circuit.select(&configured, probe_at);

        circuit.observe(
            &probe.enabled,
            &BTreeMap::from([("standard".to_owned(), "healthy".to_owned())]),
            false,
            probe_at,
        );

        assert_eq!(
            circuit
                .select(&configured, probe_at + Duration::from_secs(1))
                .enabled,
            configured
        );
    }
}
