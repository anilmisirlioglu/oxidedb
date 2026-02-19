use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Node status in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Healthy,
    Warmup,
    Unhealthy,
    Failed,
    /// Node has been failed over (automatic or manual) — awaiting recovery
    FailedOver,
}

/// Services running on a node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeService {
    Data,
    Query,
    Index,
    Xdcr,
}

/// Server Group — rack/zone awareness for replica placement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerGroup {
    pub name: String,
    pub uuid: String,
    pub nodes: Vec<String>,
}

/// Represents a node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub name: String,
    pub hostname: String,
    pub port: u16,
    pub status: NodeStatus,
    pub services: Vec<NodeService>,
    pub memory_total_mb: u64,
    pub memory_used_mb: u64,
    pub uptime_seconds: u64,
    pub joined_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    /// Server group this node belongs to (rack/zone awareness)
    #[serde(default = "default_server_group")]
    pub server_group: String,
}

fn default_server_group() -> String {
    "Group 1".to_string()
}

impl ClusterNode {
    pub fn new_self(name: String, hostname: String, port: u16) -> Self {
        let now = Utc::now();
        Self {
            name,
            hostname,
            port,
            status: NodeStatus::Healthy,
            services: vec![
                NodeService::Data,
                NodeService::Query,
                NodeService::Index,
                NodeService::Xdcr,
            ],
            memory_total_mb: 0,
            memory_used_mb: 0,
            uptime_seconds: 0,
            joined_at: now,
            last_heartbeat: now,
            server_group: "Group 1".to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn new_with_group(name: String, hostname: String, port: u16, server_group: String) -> Self {
        let mut node = Self::new_self(name, hostname, port);
        node.server_group = server_group;
        node
    }

    #[allow(dead_code)]
    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.hostname, self.port)
    }
}
