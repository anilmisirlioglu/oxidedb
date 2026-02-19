use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use tracing::info;

/// Configuration for automatic failover
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// Whether auto-failover is enabled
    pub enabled: bool,
    /// Timeout in seconds before a node is considered failed (default: 120s like Couchbase)
    pub timeout_secs: u64,
    /// Maximum number of sequential auto-failovers before requiring manual intervention
    pub max_count: u32,
    /// Whether to auto-failover nodes with no replicas (data loss risk)
    pub failover_on_data_loss: bool,
    /// Minimum number of nodes that must remain healthy for auto-failover to proceed
    pub min_cluster_size: usize,
    /// Cooldown period in seconds between auto-failover events
    pub cooldown_secs: u64,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_secs: 120,
            max_count: 3,
            failover_on_data_loss: false,
            min_cluster_size: 2,
            cooldown_secs: 30,
        }
    }
}

/// Type of failover
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailoverType {
    /// Automatic failover triggered by health monitor
    Automatic,
    /// Graceful failover initiated by operator (waits for replica promotion)
    Graceful,
    /// Hard/forced failover initiated by operator (immediate)
    Hard,
}

/// A single failover event in the history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverEvent {
    pub id: String,
    pub node_name: String,
    pub failover_type: FailoverType,
    pub reason: String,
    pub vbuckets_affected: usize,
    pub replicas_promoted: usize,
    pub rebalance_triggered: bool,
    pub timestamp: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub success: bool,
}

/// Current failover state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverState {
    /// Current configuration
    pub config: FailoverConfig,
    /// Number of auto-failovers since last reset
    pub auto_failover_count: u32,
    /// Whether auto-failover quota is exhausted
    pub quota_exhausted: bool,
    /// Nodes currently in failover state
    pub failed_over_nodes: Vec<String>,
    /// Recent failover events (capped at 50)
    pub events: Vec<FailoverEvent>,
    /// Last auto-failover timestamp
    pub last_auto_failover: Option<DateTime<Utc>>,
}

/// Manages automatic failover detection and execution
pub struct FailoverManager {
    pub config: FailoverConfig,
    pub auto_failover_count: u32,
    pub failed_over_nodes: Vec<String>,
    pub events: VecDeque<FailoverEvent>,
    pub last_auto_failover: Option<DateTime<Utc>>,
    /// Track when each node was first detected as unhealthy
    pub node_failure_timers: std::collections::HashMap<String, DateTime<Utc>>,
}

impl FailoverManager {
    pub fn new(config: FailoverConfig) -> Self {
        Self {
            config,
            auto_failover_count: 0,
            failed_over_nodes: Vec::new(),
            events: VecDeque::with_capacity(50),
            last_auto_failover: None,
            node_failure_timers: std::collections::HashMap::new(),
        }
    }

    /// Check if auto-failover can proceed for a given node
    pub fn can_auto_failover(&self, _node_name: &str, healthy_count: usize) -> (bool, String) {
        if !self.config.enabled {
            return (false, "Auto-failover is disabled".to_string());
        }

        if self.auto_failover_count >= self.config.max_count {
            return (
                false,
                format!(
                    "Auto-failover quota exhausted ({}/{}). Reset required.",
                    self.auto_failover_count, self.config.max_count
                ),
            );
        }

        // Check cooldown
        if let Some(last) = self.last_auto_failover {
            let elapsed = (Utc::now() - last).num_seconds() as u64;
            if elapsed < self.config.cooldown_secs {
                return (
                    false,
                    format!(
                        "Cooldown active: {}s remaining",
                        self.config.cooldown_secs - elapsed
                    ),
                );
            }
        }

        // Check minimum cluster size (healthy_count is AFTER failover would remove one more)
        if healthy_count < self.config.min_cluster_size {
            return (
                false,
                format!(
                    "Would leave only {} healthy nodes (minimum: {})",
                    healthy_count, self.config.min_cluster_size
                ),
            );
        }

        (true, "OK".to_string())
    }

    /// Start tracking a node's failure timer
    pub fn start_failure_timer(&mut self, node_name: &str) {
        if !self.node_failure_timers.contains_key(node_name) {
            self.node_failure_timers
                .insert(node_name.to_string(), Utc::now());
            info!(
                "Failover timer started for node '{}' (timeout: {}s)",
                node_name, self.config.timeout_secs
            );
        }
    }

    /// Clear failure timer for a node (it recovered)
    pub fn clear_failure_timer(&mut self, node_name: &str) {
        if self.node_failure_timers.remove(node_name).is_some() {
            info!("Failover timer cleared for node '{}' (node recovered)", node_name);
        }
    }

    /// Check if a node's failure timer has expired (should be failed over)
    pub fn is_failure_timeout_reached(&self, node_name: &str) -> bool {
        if let Some(started) = self.node_failure_timers.get(node_name) {
            let elapsed = (Utc::now() - *started).num_seconds() as u64;
            elapsed >= self.config.timeout_secs
        } else {
            false
        }
    }

    /// Get seconds elapsed on failure timer for a node
    pub fn failure_timer_elapsed(&self, node_name: &str) -> Option<u64> {
        self.node_failure_timers
            .get(node_name)
            .map(|started| (Utc::now() - *started).num_seconds() as u64)
    }

    /// Record a failover event
    pub fn record_failover(
        &mut self,
        node_name: &str,
        failover_type: FailoverType,
        reason: String,
        vbuckets_affected: usize,
        replicas_promoted: usize,
        rebalance_triggered: bool,
    ) -> FailoverEvent {
        let event = FailoverEvent {
            id: uuid::Uuid::new_v4().to_string(),
            node_name: node_name.to_string(),
            failover_type,
            reason: reason.clone(),
            vbuckets_affected,
            replicas_promoted,
            rebalance_triggered,
            timestamp: Utc::now(),
            completed_at: Some(Utc::now()),
            success: true,
        };

        // Update state
        if failover_type == FailoverType::Automatic {
            self.auto_failover_count += 1;
            self.last_auto_failover = Some(Utc::now());
        }

        if !self.failed_over_nodes.contains(&node_name.to_string()) {
            self.failed_over_nodes.push(node_name.to_string());
        }

        // Remove from failure timers
        self.node_failure_timers.remove(node_name);

        // Add to history (cap at 50)
        if self.events.len() >= 50 {
            self.events.pop_front();
        }
        self.events.push_back(event.clone());

        info!(
            "Failover recorded: node='{}', type={:?}, reason='{}', vBuckets={}, replicas_promoted={}",
            node_name, failover_type, reason, vbuckets_affected, replicas_promoted
        );

        event
    }

    /// Recover a failed-over node (add it back)
    pub fn recover_node(&mut self, node_name: &str) -> bool {
        if let Some(pos) = self.failed_over_nodes.iter().position(|n| n == node_name) {
            self.failed_over_nodes.remove(pos);
            self.node_failure_timers.remove(node_name);
            info!("Node '{}' recovered from failover", node_name);
            true
        } else {
            false
        }
    }

    /// Reset auto-failover counter
    pub fn reset_quota(&mut self) {
        self.auto_failover_count = 0;
        info!("Auto-failover quota reset");
    }

    /// Update configuration
    pub fn update_config(&mut self, config: FailoverConfig) {
        info!(
            "Failover config updated: enabled={}, timeout={}s, max_count={}, cooldown={}s",
            config.enabled, config.timeout_secs, config.max_count, config.cooldown_secs
        );
        self.config = config;
    }

    /// Get current state snapshot
    pub fn get_state(&self) -> FailoverState {
        FailoverState {
            config: self.config.clone(),
            auto_failover_count: self.auto_failover_count,
            quota_exhausted: self.auto_failover_count >= self.config.max_count,
            failed_over_nodes: self.failed_over_nodes.clone(),
            events: self.events.iter().cloned().collect(),
            last_auto_failover: self.last_auto_failover,
        }
    }

    /// Check if a node is in failover state
    pub fn is_failed_over(&self, node_name: &str) -> bool {
        self.failed_over_nodes.iter().any(|n| n == node_name)
    }
}
