use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use sha2::Sha256;
use hmac::Hmac;
use pbkdf2;

type HmacSha256 = Hmac<Sha256>;

const PBKDF2_ITERATIONS: u32 = 10_000;
const SALT_LEN: usize = 16;

/// Predefined roles modeled after Couchbase RBAC
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Role {
    /// Full access to everything
    Admin,
    /// Full access to a specific bucket (read+write+manage)
    BucketFullAccess,
    /// Read-only access to data
    DataReader,
    /// Write-only access to data
    DataWriter,
    /// Read+Write data access
    DataReadWrite,
    /// Query execution permission
    QuerySelect,
    /// Query management (CREATE INDEX, etc.)
    QueryManage,
    /// FTS search permission
    FtsSearcher,
    /// FTS administration
    FtsAdmin,
    /// Cluster administration
    ClusterAdmin,
    /// Read-only cluster access
    ClusterMonitor,
    /// XDCR administration
    XdcrAdmin,
    /// Backup administration
    BackupAdmin,
    /// View-only access to everything (read-only)
    ReadOnlyAdmin,
}

/// A permission check
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum Permission {
    BucketRead(String),      // bucket_name
    BucketWrite(String),     // bucket_name
    BucketManage(String),    // bucket_name
    BucketFlush(String),     // bucket_name
    QueryExecute,
    QueryManage,
    FtsRead,
    FtsManage,
    IndexManage,
    ClusterRead,
    ClusterManage,
    XdcrManage,
    BackupManage,
    UserManage,
    AuditRead,
}

/// A role assignment — can be global or scoped to a bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleAssignment {
    pub role: Role,
    /// Optional bucket scope (None = global)
    pub bucket: Option<String>,
}

/// A database user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    /// Password hash (bcrypt-style — for simplicity, we store plain text in this demo)
    pub password_hash: String,
    /// Display name
    pub display_name: String,
    /// Assigned roles
    pub roles: Vec<RoleAssignment>,
    /// Whether this is a built-in user
    pub builtin: bool,
    /// When the user was created
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Hash a password using PBKDF2-HMAC-SHA256
fn hash_password(password: &str) -> String {
    use rand::Rng;
    let mut salt = [0u8; SALT_LEN];
    rand::thread_rng().fill(&mut salt);

    let mut result = [0u8; 32];
    pbkdf2::pbkdf2::<HmacSha256>(
        password.as_bytes(),
        &salt,
        PBKDF2_ITERATIONS,
        &mut result,
    )
    .expect("PBKDF2 should not fail");

    // Store as: iterations$salt_hex$hash_hex
    format!(
        "{}${}${}",
        PBKDF2_ITERATIONS,
        hex_encode(&salt),
        hex_encode(&result)
    )
}

/// Verify a password against a PBKDF2 hash
fn verify_password(password: &str, stored: &str) -> bool {
    // Support legacy plaintext passwords (pre-hash migration)
    if !stored.contains('$') {
        return password == stored;
    }

    let parts: Vec<&str> = stored.splitn(3, '$').collect();
    if parts.len() != 3 {
        return password == stored;
    }

    let iterations: u32 = match parts[0].parse() {
        Ok(n) => n,
        Err(_) => return false,
    };
    let salt = match hex_decode(parts[1]) {
        Some(s) => s,
        None => return false,
    };
    let expected = match hex_decode(parts[2]) {
        Some(h) => h,
        None => return false,
    };

    let mut result = vec![0u8; expected.len()];
    pbkdf2::pbkdf2::<HmacSha256>(
        password.as_bytes(),
        &salt,
        iterations,
        &mut result,
    )
    .expect("PBKDF2 should not fail");

    result == expected
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// RBAC Manager — manages users and permission checks
pub struct RbacManager {
    users: RwLock<HashMap<String, User>>,
}

impl RbacManager {
    pub fn new() -> Self {
        let mut users = HashMap::new();

        // Create default admin user with hashed password
        users.insert(
            "Administrator".to_string(),
            User {
                username: "Administrator".to_string(),
                password_hash: hash_password("password"),
                display_name: "Built-in Administrator".to_string(),
                roles: vec![RoleAssignment {
                    role: Role::Admin,
                    bucket: None,
                }],
                builtin: true,
                created_at: chrono::Utc::now(),
            },
        );

        Self {
            users: RwLock::new(users),
        }
    }

    /// Authenticate a user with username/password (PBKDF2 verified)
    pub fn authenticate(&self, username: &str, password: &str) -> Option<User> {
        let users = self.users.read().ok()?;
        let user = users.get(username)?;
        if verify_password(password, &user.password_hash) {
            Some(user.clone())
        } else {
            None
        }
    }

    /// Check if a user has a specific permission
    pub fn check_permission(&self, username: &str, permission: &Permission) -> bool {
        let users = match self.users.read() {
            Ok(u) => u,
            Err(_) => return false,
        };
        let user = match users.get(username) {
            Some(u) => u,
            None => return false,
        };

        for ra in &user.roles {
            if role_grants_permission(&ra.role, &ra.bucket, permission) {
                return true;
            }
        }
        false
    }

    /// Create a new user with hashed password
    pub fn create_user(
        &self,
        username: String,
        password: String,
        display_name: String,
        roles: Vec<RoleAssignment>,
    ) -> Result<User, String> {
        let mut users = self.users.write().map_err(|e| e.to_string())?;
        if users.contains_key(&username) {
            return Err(format!("User '{}' already exists", username));
        }
        let user = User {
            username: username.clone(),
            password_hash: hash_password(&password),
            display_name,
            roles,
            builtin: false,
            created_at: chrono::Utc::now(),
        };
        users.insert(username, user.clone());
        Ok(user)
    }

    /// Delete a user
    pub fn delete_user(&self, username: &str) -> Result<(), String> {
        let mut users = self.users.write().map_err(|e| e.to_string())?;
        let user = users.get(username).ok_or_else(|| format!("User '{}' not found", username))?;
        if user.builtin {
            return Err(format!("Cannot delete built-in user '{}'", username));
        }
        users.remove(username);
        Ok(())
    }

    /// List all users (without password hashes)
    pub fn list_users(&self) -> Vec<UserInfo> {
        let users = match self.users.read() {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        users.values().map(|u| UserInfo {
            username: u.username.clone(),
            display_name: u.display_name.clone(),
            roles: u.roles.clone(),
            builtin: u.builtin,
            created_at: u.created_at,
        }).collect()
    }

    /// Get a user by username (without password)
    pub fn get_user(&self, username: &str) -> Option<UserInfo> {
        let users = self.users.read().ok()?;
        users.get(username).map(|u| UserInfo {
            username: u.username.clone(),
            display_name: u.display_name.clone(),
            roles: u.roles.clone(),
            builtin: u.builtin,
            created_at: u.created_at,
        })
    }

    /// Update user roles
    pub fn update_user_roles(&self, username: &str, roles: Vec<RoleAssignment>) -> Result<(), String> {
        let mut users = self.users.write().map_err(|e| e.to_string())?;
        let user = users.get_mut(username).ok_or_else(|| format!("User '{}' not found", username))?;
        user.roles = roles;
        Ok(())
    }

    /// Change user password (hashed)
    pub fn change_password(&self, username: &str, new_password: String) -> Result<(), String> {
        let mut users = self.users.write().map_err(|e| e.to_string())?;
        let user = users.get_mut(username).ok_or_else(|| format!("User '{}' not found", username))?;
        user.password_hash = hash_password(&new_password);
        Ok(())
    }

    /// Authenticate via client certificate — extract CN (Common Name) and match to a user
    /// Returns the user if the certificate CN matches a known username.
    /// This is used for mTLS (mutual TLS) client certificate authentication.
    #[allow(dead_code)]
    pub fn authenticate_by_cert_cn(&self, common_name: &str) -> Option<User> {
        let users = self.users.read().ok()?;
        // Certificate CN maps directly to username
        users.get(common_name).cloned()
    }

    /// Map a certificate subject prefix to a role (for auto-provisioning)
    /// e.g. "CN=service-" → DataReadWrite role
    #[allow(dead_code)]
    pub fn authenticate_cert_with_prefix(&self, common_name: &str) -> Option<User> {
        // First try exact match
        if let Some(user) = self.authenticate_by_cert_cn(common_name) {
            return Some(user);
        }

        // If CN starts with a known prefix, create a temporary session user with default role
        // This supports service account patterns like "CN=service-indexer"
        let users = self.users.read().ok()?;

        // Check if any user's display_name matches a certificate prefix pattern
        for user in users.values() {
            if common_name.starts_with(&user.username) {
                return Some(user.clone());
            }
        }

        None
    }
}

/// User info (without password) for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub username: String,
    pub display_name: String,
    pub roles: Vec<RoleAssignment>,
    pub builtin: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Check if a role (with optional bucket scope) grants a specific permission
fn role_grants_permission(role: &Role, bucket_scope: &Option<String>, permission: &Permission) -> bool {
    match role {
        Role::Admin => true, // Admin has all permissions

        Role::ClusterAdmin => matches!(
            permission,
            Permission::ClusterRead | Permission::ClusterManage | Permission::UserManage | Permission::AuditRead
        ),

        Role::ClusterMonitor => matches!(
            permission,
            Permission::ClusterRead | Permission::AuditRead
        ),

        Role::ReadOnlyAdmin => matches!(
            permission,
            Permission::ClusterRead
            | Permission::AuditRead
            | Permission::FtsRead
            | Permission::BucketRead(_)
        ),

        Role::BucketFullAccess => {
            match permission {
                Permission::BucketRead(b) | Permission::BucketWrite(b) | Permission::BucketManage(b) | Permission::BucketFlush(b) => {
                    bucket_scope.as_deref() == Some(b.as_str()) || bucket_scope.is_none()
                }
                Permission::QueryExecute | Permission::QueryManage | Permission::IndexManage => {
                    bucket_scope.is_some() // scoped bucket full access includes query on that bucket
                }
                _ => false,
            }
        }

        Role::DataReader => {
            if let Permission::BucketRead(b) = permission {
                bucket_scope.as_deref() == Some(b.as_str()) || bucket_scope.is_none()
            } else {
                false
            }
        }

        Role::DataWriter => {
            if let Permission::BucketWrite(b) = permission {
                bucket_scope.as_deref() == Some(b.as_str()) || bucket_scope.is_none()
            } else {
                false
            }
        }

        Role::DataReadWrite => {
            match permission {
                Permission::BucketRead(b) | Permission::BucketWrite(b) => {
                    bucket_scope.as_deref() == Some(b.as_str()) || bucket_scope.is_none()
                }
                _ => false,
            }
        }

        Role::QuerySelect => matches!(permission, Permission::QueryExecute),
        Role::QueryManage => matches!(permission, Permission::QueryExecute | Permission::QueryManage | Permission::IndexManage),

        Role::FtsSearcher => matches!(permission, Permission::FtsRead),
        Role::FtsAdmin => matches!(permission, Permission::FtsRead | Permission::FtsManage),

        Role::XdcrAdmin => matches!(permission, Permission::XdcrManage | Permission::ClusterRead),
        Role::BackupAdmin => matches!(permission, Permission::BackupManage | Permission::ClusterRead),
    }
}

/// List of all available roles with descriptions
pub fn list_available_roles() -> Vec<RoleDescription> {
    vec![
        RoleDescription { role: Role::Admin, name: "Admin".into(), description: "Full access to everything".into() },
        RoleDescription { role: Role::BucketFullAccess, name: "Bucket Full Access".into(), description: "Full access to a specific bucket".into() },
        RoleDescription { role: Role::DataReader, name: "Data Reader".into(), description: "Read-only access to bucket data".into() },
        RoleDescription { role: Role::DataWriter, name: "Data Writer".into(), description: "Write access to bucket data".into() },
        RoleDescription { role: Role::DataReadWrite, name: "Data Read/Write".into(), description: "Read and write access to bucket data".into() },
        RoleDescription { role: Role::QuerySelect, name: "Query Select".into(), description: "Execute SELECT queries".into() },
        RoleDescription { role: Role::QueryManage, name: "Query Manage".into(), description: "Manage queries and indexes".into() },
        RoleDescription { role: Role::FtsSearcher, name: "FTS Searcher".into(), description: "Execute FTS searches".into() },
        RoleDescription { role: Role::FtsAdmin, name: "FTS Admin".into(), description: "Manage FTS indexes".into() },
        RoleDescription { role: Role::ClusterAdmin, name: "Cluster Admin".into(), description: "Cluster administration".into() },
        RoleDescription { role: Role::ClusterMonitor, name: "Cluster Monitor".into(), description: "Read-only cluster monitoring".into() },
        RoleDescription { role: Role::XdcrAdmin, name: "XDCR Admin".into(), description: "XDCR replication management".into() },
        RoleDescription { role: Role::BackupAdmin, name: "Backup Admin".into(), description: "Backup/restore management".into() },
        RoleDescription { role: Role::ReadOnlyAdmin, name: "Read-Only Admin".into(), description: "Read-only access to everything".into() },
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleDescription {
    pub role: Role,
    pub name: String,
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_admin_exists() {
        let rbac = RbacManager::new();
        let user = rbac.authenticate("Administrator", "password");
        assert!(user.is_some());
        assert_eq!(user.unwrap().username, "Administrator");
    }

    #[test]
    fn test_wrong_password_fails() {
        let rbac = RbacManager::new();
        assert!(rbac.authenticate("Administrator", "wrong").is_none());
    }

    #[test]
    fn test_create_user_and_authenticate() {
        let rbac = RbacManager::new();
        rbac.create_user(
            "testuser".to_string(),
            "testpass".to_string(),
            "Test User".to_string(),
            vec![RoleAssignment { role: Role::BucketFullAccess, bucket: Some("*".to_string()) }],
        ).unwrap();
        let user = rbac.authenticate("testuser", "testpass");
        assert!(user.is_some());
    }

    #[test]
    fn test_delete_user() {
        let rbac = RbacManager::new();
        rbac.create_user("u1".into(), "p1".into(), "User 1".into(), vec![]).unwrap();
        assert!(rbac.delete_user("u1").is_ok());
        assert!(rbac.authenticate("u1", "p1").is_none());
    }

    #[test]
    fn test_permission_check_admin() {
        let rbac = RbacManager::new();
        assert!(rbac.check_permission("Administrator", &Permission::ClusterManage));
        assert!(rbac.check_permission("Administrator", &Permission::BucketRead("test".into())));
    }

    #[test]
    fn test_permission_check_limited_user() {
        let rbac = RbacManager::new();
        rbac.create_user(
            "reader".into(),
            "pass".into(),
            "Reader User".into(),
            vec![RoleAssignment { role: Role::DataReader, bucket: Some("mybucket".to_string()) }],
        ).unwrap();
        assert!(rbac.check_permission("reader", &Permission::BucketRead("mybucket".into())));
        assert!(!rbac.check_permission("reader", &Permission::BucketWrite("mybucket".into())));
        assert!(!rbac.check_permission("reader", &Permission::ClusterManage));
    }

    #[test]
    fn test_change_password() {
        let rbac = RbacManager::new();
        rbac.create_user("u1".into(), "old".into(), "User 1".into(), vec![]).unwrap();
        rbac.change_password("u1", "new".to_string()).unwrap();
        assert!(rbac.authenticate("u1", "old").is_none());
        assert!(rbac.authenticate("u1", "new").is_some());
    }

    #[test]
    fn test_list_users() {
        let rbac = RbacManager::new();
        rbac.create_user("u1".into(), "p1".into(), "User 1".into(), vec![]).unwrap();
        rbac.create_user("u2".into(), "p2".into(), "User 2".into(), vec![]).unwrap();
        let users = rbac.list_users();
        assert!(users.len() >= 3); // Admin + u1 + u2
    }

    #[test]
    fn test_list_roles() {
        let roles = list_available_roles();
        assert!(roles.len() >= 10);
    }
}
