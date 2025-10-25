// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use tracing::{trace, warn};

/// Special group name that includes all users
pub const ANY_GROUP_NAME: &str = "@any";

/// Authentication method for users
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    /// SHA256 hashed password with optional salt
    Sha256,
    /// Plain text password (not recommended for production)
    PlainPassword,
    /// Client certificate authentication
    ClientCert,
    /// Anonymous user (no credentials required)
    Anonymous,
    /// Unauthenticated user (connection allowed without authentication)
    Unauthenticated,
}

/// Authentication entry for a user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationEntry {
    /// Username
    pub name: String,
    /// Authentication method
    pub method: AuthMethod,
    /// Password digest (SHA256 hash or plain password)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Alternative field name for plain_password method
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Salt for SHA256 hashing
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub salt: String,
}

/// Authorization type (allow or deny)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationType {
    Allow,
    Deny,
    None,
}

/// Authorization rule for a topic
#[derive(Debug, Clone)]
pub struct Authorization {
    /// Topic filter (may contain wildcards)
    pub topic: String,
    /// Topic filter tokens (split by '/')
    pub topic_tokens: Vec<String>,
    /// Rule number (higher number = higher priority)
    pub rule_nr: usize,
    /// Subscribe authorization type
    pub sub_type: AuthorizationType,
    /// Users/groups allowed/denied to subscribe
    pub sub_perm: HashSet<String>,
    /// Publish authorization type
    pub pub_type: AuthorizationType,
    /// Users/groups allowed/denied to publish
    pub pub_perm: HashSet<String>,
}

/// Authorization entry for JSON deserialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationEntry {
    /// Topic filter
    pub topic: String,
    /// Allow rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow: Option<AuthorizationAllow>,
    /// Deny rules
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny: Option<AuthorizationDeny>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationAllow {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sub: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "pub")]
    pub publish: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationDeny {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sub: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "pub")]
    pub publish: Vec<String>,
}

/// Group definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    /// Group name (must start with '@')
    pub name: String,
    /// Members of the group
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<String>,
}

/// Root configuration structure for JSON deserialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication entries
    pub authentication: Vec<AuthenticationEntry>,
    /// Group definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group: Vec<Group>,
    /// Authorization rules
    pub authorization: Vec<AuthorizationEntry>,
}

/// Main authentication and authorization manager
pub struct Security {
    /// Authentication map: username -> AuthenticationEntry
    authentication: HashMap<String, AuthenticationEntry>,
    /// Groups map: group_name -> Group
    groups: HashMap<String, Group>,
    /// Authorization rules (ordered by rule_nr)
    authorization: Vec<Authorization>,
    /// Anonymous username (if configured)
    anonymous: Option<String>,
    /// Unauthenticated username (if configured)
    unauthenticated: Option<String>,
}

impl Security {
    /// Create a new empty Security instance
    pub fn new() -> Self {
        Self {
            authentication: HashMap::new(),
            groups: HashMap::new(),
            authorization: Vec::new(),
            anonymous: None,
            unauthenticated: None,
        }
    }

    /// Create default configuration (anonymous user with full access)
    pub fn default_config() -> Result<Self> {
        let mut security = Self::new();

        // Add anonymous user
        let username = "anonymous".to_string();
        security.authentication.insert(
            username.clone(),
            AuthenticationEntry {
                name: username.clone(),
                method: AuthMethod::Anonymous,
                digest: None,
                password: None,
                salt: String::new(),
            },
        );
        security.anonymous = Some(username.clone());

        // Add @any group
        security.groups.insert(
            ANY_GROUP_NAME.to_string(),
            Group {
                name: ANY_GROUP_NAME.to_string(),
                members: Vec::new(),
            },
        );

        // Add authorization rule for "#" (all topics)
        let topic = "#".to_string();
        let rule_nr = 0;
        let topic_tokens = Self::get_topic_filter_tokens(&topic);

        let mut sub_perm = HashSet::new();
        sub_perm.insert(username.clone());

        let mut pub_perm = HashSet::new();
        pub_perm.insert(username);

        security.authorization.push(Authorization {
            topic,
            topic_tokens,
            rule_nr,
            sub_type: AuthorizationType::Allow,
            sub_perm,
            pub_type: AuthorizationType::Allow,
            pub_perm,
        });

        security.validate()?;
        Ok(security)
    }

    /// Load configuration from JSON file
    pub fn load_json(path: &str) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let json_content = Self::remove_comments(reader)?;

        let config: AuthConfig = json5::from_str(&json_content)?;

        let mut security = Self::new();

        // Add @any group
        security.groups.insert(
            ANY_GROUP_NAME.to_string(),
            Group {
                name: ANY_GROUP_NAME.to_string(),
                members: Vec::new(),
            },
        );

        // Process authentication entries
        for auth_entry in config.authentication {
            let name = auth_entry.name.clone();
            if !Self::is_valid_user_name(&name) {
                return Err(anyhow!("Invalid username: {name}"));
            }

            // Handle password field for plain_password method
            let digest = if auth_entry.method == AuthMethod::PlainPassword {
                auth_entry.password.or(auth_entry.digest)
            } else {
                auth_entry.digest
            };

            match auth_entry.method {
                AuthMethod::Anonymous => {
                    if security.anonymous.is_some() {
                        return Err(anyhow!("Only one anonymous user can be configured"));
                    }
                    security.anonymous = Some(name.clone());
                }
                AuthMethod::Unauthenticated => {
                    if security.unauthenticated.is_some() {
                        return Err(anyhow!("Only one unauthenticated user can be configured"));
                    }
                    security.unauthenticated = Some(name.clone());
                }
                _ => {}
            }

            security.authentication.insert(
                name.clone(),
                AuthenticationEntry {
                    name,
                    method: auth_entry.method,
                    digest,
                    password: None,
                    salt: auth_entry.salt,
                },
            );
        }

        // Process groups
        for group in config.group {
            let name = group.name.clone();
            if !Self::is_valid_group_name(&name) {
                return Err(anyhow!("Invalid group name: {name}"));
            }

            for member in &group.members {
                if !Self::is_valid_user_name(member) {
                    return Err(anyhow!("Invalid username in group {name}: {member}"));
                }
            }

            security.groups.insert(name, group);
        }

        // Process authorization rules
        for (idx, auth_entry) in config.authorization.into_iter().enumerate() {
            let topic = auth_entry.topic;
            let topic_tokens = Self::get_topic_filter_tokens(&topic);
            let rule_nr = idx;

            let mut sub_perm = HashSet::new();
            let mut sub_type = AuthorizationType::None;
            let mut pub_perm = HashSet::new();
            let mut pub_type = AuthorizationType::None;

            if let Some(allow) = auth_entry.allow {
                if !allow.sub.is_empty() {
                    sub_type = AuthorizationType::Allow;
                    for username in allow.sub {
                        sub_perm.insert(username);
                    }
                }
                if !allow.publish.is_empty() {
                    pub_type = AuthorizationType::Allow;
                    for username in allow.publish {
                        pub_perm.insert(username);
                    }
                }
            }

            if let Some(deny) = auth_entry.deny {
                if !deny.sub.is_empty() {
                    sub_type = AuthorizationType::Deny;
                    for username in deny.sub {
                        sub_perm.insert(username);
                    }
                }
                if !deny.publish.is_empty() {
                    pub_type = AuthorizationType::Deny;
                    for username in deny.publish {
                        pub_perm.insert(username);
                    }
                }
            }

            security.authorization.push(Authorization {
                topic,
                topic_tokens,
                rule_nr,
                sub_type,
                sub_perm,
                pub_type,
                pub_perm,
            });
        }

        security.validate()?;
        Ok(security)
    }

    /// Remove C++ style comments from JSON content
    pub fn remove_comments<R: BufRead>(reader: R) -> Result<String> {
        let mut result = String::new();
        let mut inside_comment = false;
        let mut inside_block_comment = false;
        let mut inside_single_quote = false;
        let mut inside_double_quote = false;

        let content = reader
            .bytes()
            .collect::<std::io::Result<Vec<u8>>>()?;
        let mut chars = content.iter().peekable();

        while let Some(&byte) = chars.next() {
            let c = byte as char;

            // Check for comment start
            if !inside_comment
                && !inside_block_comment
                && !inside_double_quote
                && !inside_single_quote
                && c == '/'
            {
                if let Some(&&next_byte) = chars.peek() {
                    let next = next_byte as char;
                    if next == '/' {
                        inside_comment = true;
                        chars.next(); // consume '/'
                        continue;
                    } else if next == '*' {
                        inside_block_comment = true;
                        chars.next(); // consume '*'
                        continue;
                    }
                }
            }

            // Check for block comment end
            if inside_block_comment && c == '*' {
                if let Some(&&next_byte) = chars.peek() {
                    let next = next_byte as char;
                    if next == '/' {
                        inside_block_comment = false;
                        chars.next(); // consume '/'
                        continue;
                    }
                }
            }

            // End line comment on newline
            if c == '\n' {
                inside_comment = false;
            }

            // Add character if not in comment
            if !inside_comment && !inside_block_comment {
                // Track quote state
                if !inside_double_quote && !inside_single_quote && c == '\'' {
                    inside_single_quote = !inside_single_quote;
                }
                if !inside_single_quote && c == '"' {
                    inside_double_quote = !inside_double_quote;
                }
                result.push(c);
            }
        }

        Ok(result)
    }

    /// Validate group name (must start with '@')
    pub fn is_valid_group_name(name: &str) -> bool {
        !name.is_empty() && name.starts_with('@')
    }

    /// Validate user name (must NOT start with '@')
    pub fn is_valid_user_name(name: &str) -> bool {
        !name.is_empty() && !name.starts_with('@')
    }

    /// Split topic filter into tokens
    pub fn get_topic_filter_tokens(topic_filter: &str) -> Vec<String> {
        topic_filter.split('/').map(|s| s.to_string()).collect()
    }

    /// Get next rule number
    fn get_next_rule_nr(&self) -> usize {
        self.authorization
            .iter()
            .map(|a| a.rule_nr)
            .max()
            .unwrap_or(0)
            + 1
    }

    /// Validate configuration
    fn validate(&self) -> Result<()> {
        // Validate group members exist
        for (group_name, group) in &self.groups {
            if group_name == ANY_GROUP_NAME {
                continue;
            }
            for member in &group.members {
                if Self::is_valid_user_name(member) && !self.authentication.contains_key(member) {
                    return Err(anyhow!(
                        "Invalid username in group {group_name}: {member}"
                    ));
                }
            }
        }

        // Warn about unsalted SHA256 passwords
        let unsalted: Vec<&String> = self
            .authentication
            .iter()
            .filter(|(_, auth)| auth.method == AuthMethod::Sha256 && auth.salt.is_empty())
            .map(|(name, _)| name)
            .collect();

        if !unsalted.is_empty() {
            warn!(
                "The following users have no salt specified: {}",
                unsalted
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        // Validate authorization entries
        for auth in &self.authorization {
            for username in auth.sub_perm.iter().chain(auth.pub_perm.iter()) {
                if Self::is_valid_group_name(username) && !self.groups.contains_key(username) {
                    return Err(anyhow!(
                        "Invalid group name for topic {}: {username}",
                        auth.topic
                    ));
                }
                if Self::is_valid_user_name(username) && !self.authentication.contains_key(username)
                {
                    return Err(anyhow!(
                        "Invalid username for topic {}: {username}",
                        auth.topic
                    ));
                }
            }
        }

        Ok(())
    }

    /// Get anonymous username
    pub fn login_anonymous(&self) -> Option<&str> {
        self.anonymous.as_deref()
    }

    /// Get unauthenticated username
    pub fn login_unauthenticated(&self) -> Option<&str> {
        self.unauthenticated.as_deref()
    }

    /// Calculate SHA256 hash
    pub fn sha256hash(message: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(message.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }

    /// Authenticate user with client certificate
    pub fn login_cert(&self, username: &str) -> bool {
        if let Some(auth) = self.authentication.get(username) {
            auth.method == AuthMethod::ClientCert
        } else {
            false
        }
    }

    /// Authenticate user with username and password
    pub fn login(&self, username: &str, password: &str) -> Option<String> {
        if let Some(auth) = self.authentication.get(username) {
            match auth.method {
                AuthMethod::Sha256 => {
                    if let Some(ref digest) = auth.digest {
                        let hash = Self::sha256hash(&format!("{}{password}", auth.salt));
                        if digest.eq_ignore_ascii_case(&hash) {
                            return Some(username.to_string());
                        }
                    }
                }
                AuthMethod::PlainPassword => {
                    if let Some(ref digest) = auth.digest {
                        if digest == password {
                            return Some(username.to_string());
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Get user's groups (including @any)
    fn get_user_groups(&self, username: &str) -> HashSet<String> {
        let mut groups = HashSet::new();
        groups.insert(username.to_string());

        for (group_name, group) in &self.groups {
            if group_name == ANY_GROUP_NAME || group.members.contains(&username.to_string()) {
                groups.insert(group_name.clone());
            }
        }

        groups
    }

    /// Check publish authorization
    pub fn auth_pub(&self, topic: &str, username: &str) -> AuthorizationType {
        let user_groups = self.get_user_groups(username);
        let mut result_type = AuthorizationType::Deny;
        let mut priority = 0;

        let topic_tokens = Self::get_topic_filter_tokens(topic);

        for auth in &self.authorization {
            if auth.pub_type == AuthorizationType::None {
                continue;
            }

            // Check if topic matches the authorization topic filter
            if Self::topic_matches(&topic_tokens, &auth.topic_tokens) {
                // Check if user or any of their groups is in the authorization list
                for allowed_user in &auth.pub_perm {
                    if user_groups.contains(allowed_user) {
                        if auth.rule_nr >= priority {
                            result_type = auth.pub_type;
                            priority = auth.rule_nr;
                        }
                    }
                }
            }
        }

        result_type
    }

    /// Check subscribe authorization
    pub fn auth_sub(&self, topic_filter: &str, username: &str) -> AuthorizationType {
        let user_groups = self.get_user_groups(username);
        let mut result_type = AuthorizationType::Deny;
        let mut priority = 0;

        let subscription_tokens = Self::get_topic_filter_tokens(topic_filter);

        for auth in &self.authorization {
            if auth.sub_type == AuthorizationType::None {
                continue;
            }

            // For subscribe, we need wildcard matching logic
            if let Some(_matched_topic) =
                Self::is_subscribe_allowed(&auth.topic_tokens, &subscription_tokens)
            {
                // Check if user or any of their groups is in the authorization list
                for allowed_user in &auth.sub_perm {
                    if user_groups.contains(allowed_user) {
                        if auth.rule_nr >= priority {
                            result_type = auth.sub_type;
                            priority = auth.rule_nr;
                        }
                    }
                }
            }
        }

        result_type
    }

    /// Check if a topic matches a topic filter (for publish authorization)
    fn topic_matches(topic_tokens: &[String], filter_tokens: &[String]) -> bool {
        let mut topic_iter = topic_tokens.iter();
        let mut filter_iter = filter_tokens.iter();

        loop {
            match (topic_iter.next(), filter_iter.next()) {
                (Some(topic_level), Some(filter_level)) => {
                    if filter_level == "#" {
                        return true; // Multi-level wildcard matches everything
                    }
                    if filter_level == "+" {
                        continue; // Single-level wildcard matches any single level
                    }
                    if topic_level != filter_level {
                        return false; // Literal mismatch
                    }
                }
                (None, Some(filter_level)) => {
                    // Topic ended, but filter has more levels
                    return filter_level == "#";
                }
                (Some(_), None) => {
                    // Filter ended, but topic has more levels
                    return false;
                }
                (None, None) => {
                    // Both ended at the same time
                    return true;
                }
            }
        }
    }

    /// Check if subscription is allowed (complex wildcard matching logic)
    pub fn is_subscribe_allowed(
        authorized_filter: &[String],
        subscription_filter: &[String],
    ) -> Option<String> {
        let mut result = String::new();
        let mut auth_iter = authorized_filter.iter();
        let mut sub_iter = subscription_filter.iter();

        loop {
            match (auth_iter.next(), sub_iter.next()) {
                (Some(auth), Some(sub)) => {
                    if auth == "#" {
                        // Auth has #, consume rest of subscription
                        if !result.is_empty() {
                            result.push('/');
                        }
                        result.push_str(sub);
                        for remaining in sub_iter {
                            result.push('/');
                            result.push_str(remaining);
                        }
                        return Some(result);
                    }

                    if sub == "#" {
                        // Subscription has #, consume rest of authorized filter
                        if !result.is_empty() {
                            result.push('/');
                        }
                        result.push_str(auth);
                        for remaining in auth_iter {
                            result.push('/');
                            result.push_str(remaining);
                        }
                        return Some(result);
                    }

                    if auth == "+" {
                        // Auth has +, use subscription level
                        if !result.is_empty() {
                            result.push('/');
                        }
                        result.push_str(sub);
                    } else if sub == "+" {
                        // Subscription has +, use auth level
                        if !result.is_empty() {
                            result.push('/');
                        }
                        result.push_str(auth);
                    } else if auth == sub {
                        // Literal match
                        if !result.is_empty() {
                            result.push('/');
                        }
                        result.push_str(auth);
                    } else {
                        // Mismatch
                        return None;
                    }
                }
                (None, None) => {
                    // Both ended at the same time
                    return Some(result);
                }
                _ => {
                    // Length mismatch
                    return None;
                }
            }
        }
    }

    /// Get authorized topics for a user's subscription
    pub fn get_auth_sub_topics(&self, username: &str, topic_filter: &str) -> Vec<String> {
        let user_groups = self.get_user_groups(username);
        let subscription_tokens = Self::get_topic_filter_tokens(topic_filter);
        let mut auth_topics = Vec::new();

        for auth in &self.authorization {
            if auth.sub_type == AuthorizationType::None {
                continue;
            }

            // Check if user is in the authorization list
            let user_in_auth = auth.sub_perm.iter().any(|u| user_groups.contains(u));
            if !user_in_auth {
                continue;
            }

            if auth.sub_type == AuthorizationType::Allow {
                if let Some(matched_topic) =
                    Self::is_subscribe_allowed(&auth.topic_tokens, &subscription_tokens)
                {
                    auth_topics.push(matched_topic);
                }
            } else {
                // Deny: remove matching topics
                auth_topics.retain(|topic| {
                    let topic_tokens = Self::get_topic_filter_tokens(topic);
                    !Self::is_subscribe_denied(&auth.topic_tokens, &topic_tokens)
                });
            }
        }

        auth_topics
    }

    /// Check if subscription is denied
    pub fn is_subscribe_denied(deny_filter: &[String], subscription_filter: &[String]) -> bool {
        let mut deny_iter = deny_filter.iter();
        let mut sub_iter = subscription_filter.iter();

        loop {
            match (deny_iter.next(), sub_iter.next()) {
                (Some(deny), Some(sub)) => {
                    if deny == sub {
                        continue; // Exact match, continue checking
                    }

                    if deny == "#" {
                        return true; // Deny has #, everything matches
                    }

                    if sub == "#" {
                        return false; // Sub has #, but deny doesn't match
                    }

                    if deny == "+" {
                        continue; // Deny has +, matches any single level
                    }

                    // Mismatch
                    return false;
                }
                (None, None) => {
                    // Both ended at the same time - full match
                    return true;
                }
                _ => {
                    // Length mismatch
                    return false;
                }
            }
        }
    }

    /// Check if user is authorized to subscribe to a topic filter
    pub fn is_subscribe_authorized(&self, username: &str, topic_filter: &str) -> bool {
        !self.get_auth_sub_topics(username, topic_filter).is_empty()
    }
}

impl Default for Security {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_names() {
        assert!(Security::is_valid_user_name("user1"));
        assert!(!Security::is_valid_user_name("@group"));
        assert!(!Security::is_valid_user_name(""));

        assert!(Security::is_valid_group_name("@group"));
        assert!(!Security::is_valid_group_name("user1"));
        assert!(!Security::is_valid_group_name(""));
    }

    #[test]
    fn test_sha256hash() {
        let hash = Security::sha256hash("password");
        assert_eq!(hash.len(), 64); // SHA256 produces 64 hex characters
    }

    #[test]
    fn test_get_topic_filter_tokens() {
        let tokens = Security::get_topic_filter_tokens("a/b/c");
        assert_eq!(tokens, vec!["a", "b", "c"]);

        let tokens = Security::get_topic_filter_tokens("a/+/c");
        assert_eq!(tokens, vec!["a", "+", "c"]);

        let tokens = Security::get_topic_filter_tokens("a/#");
        assert_eq!(tokens, vec!["a", "#"]);
    }

    #[test]
    fn test_topic_matches() {
        let topic = Security::get_topic_filter_tokens("a/b/c");
        let filter = Security::get_topic_filter_tokens("a/b/c");
        assert!(Security::topic_matches(&topic, &filter));

        let topic = Security::get_topic_filter_tokens("a/b/c");
        let filter = Security::get_topic_filter_tokens("a/+/c");
        assert!(Security::topic_matches(&topic, &filter));

        let topic = Security::get_topic_filter_tokens("a/b/c");
        let filter = Security::get_topic_filter_tokens("a/#");
        assert!(Security::topic_matches(&topic, &filter));

        let topic = Security::get_topic_filter_tokens("a/b/c");
        let filter = Security::get_topic_filter_tokens("a/b/d");
        assert!(!Security::topic_matches(&topic, &filter));
    }

    #[test]
    fn test_remove_comments() {
        let input = r#"
        {
            // This is a comment
            "key": "value", // Another comment
            /* Block comment */
            "key2": "value2"
        }
        "#;
        let result = Security::remove_comments(std::io::Cursor::new(input)).unwrap();
        assert!(!result.contains("//"));
        assert!(!result.contains("/*"));
        assert!(result.contains("\"key\""));
        assert!(result.contains("\"value\""));
    }
}
