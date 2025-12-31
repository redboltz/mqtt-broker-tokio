// MIT License
//
// Copyright (c) 2025 Takatoshi Kondo
//
// SPDX-License-Identifier: MIT

// Include auth_impl module for testing
mod auth_impl {
    include!("../src/auth_impl.rs");
}

use auth_impl::{AuthorizationType, Security};
use std::io::Cursor;

#[test]
fn test_json_remove_comments() {
    let input = "test";
    let result = Security::remove_comments(Cursor::new(input)).unwrap();
    assert_eq!(result, "test");

    let input = "//test\ntest";
    let result = Security::remove_comments(Cursor::new(input)).unwrap();
    assert_eq!(result, "\ntest");

    let input = "'//test'";
    let result = Security::remove_comments(Cursor::new(input)).unwrap();
    assert_eq!(result, "'//test'");

    let input = "\"//test\"";
    let result = Security::remove_comments(Cursor::new(input)).unwrap();
    assert_eq!(result, "\"//test\"");

    let input = "\"'//test'\"";
    let result = Security::remove_comments(Cursor::new(input)).unwrap();
    assert_eq!(result, "\"'//test'\"");

    let input = "'\"//test\"'";
    let result = Security::remove_comments(Cursor::new(input)).unwrap();
    assert_eq!(result, "'\"//test\"'");

    let input = "";
    let result = Security::remove_comments(Cursor::new(input)).unwrap();
    assert_eq!(result, "");
}

#[test]
fn test_default_config() {
    let security = Security::default_config().unwrap();

    // Check anonymous user exists
    assert_eq!(security.login_anonymous(), Some("anonymous"));

    // Check pub authorization for anonymous user
    assert_eq!(
        security.auth_pub("topic", "anonymous"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_pub("sub/topic", "anonymous"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_pub("sub/topic1", "anonymous"),
        AuthorizationType::Allow
    );

    // Check sub authorization for anonymous user
    assert_eq!(
        security.auth_sub("topic", "anonymous"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_sub("sub/topic", "anonymous"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_sub("sub/topic1", "anonymous"),
        AuthorizationType::Allow
    );
}

#[test]
fn test_json_load() {
    let json_content = r##"{
        "authentication": [
            {
                "name": "u1",
                "method": "sha256",
                "salt": "salt",
                "digest": "38ea2e5e88fcd692fe177c6cada15e9b2db6e70bee0a0d6678c8d3b2a9aae2ad"
            },
            {
                "name": "u2",
                "method": "client_cert"
            },
            {
                "name": "u3",
                "method": "plain_password",
                "password": "mypassword"
            },
            {
                "name": "anonymous",
                "method": "anonymous"
            }
        ],
        "group": [
            {
                "name": "@g1",
                "members": ["u1", "u2", "anonymous"]
            }
        ],
        "authorization": [
            {
                "topic": "#",
                "allow": { "pub": ["@g1"] }
            },
            {
                 "topic": "#",
                 "deny": { "sub": ["@g1"] }
            },
            {
                "topic": "sub/#",
                 "allow": {
                    "sub": ["@g1"],
                    "pub": ["@g1"]
                 }
            },
            {
                "topic": "sub/topic1",
                "deny": {
                    "sub": ["u1", "anonymous"],
                    "pub": ["u1", "anonymous"]
                }
            }
        ]
    }"##;

    // Write JSON to temp file
    let temp_file = std::env::temp_dir().join("test_auth.json");
    std::fs::write(&temp_file, json_content).unwrap();

    let security = Security::load_json(temp_file.to_str().unwrap()).unwrap();

    // Check anonymous user
    assert_eq!(security.login_anonymous(), Some("anonymous"));

    // Check SHA256 authentication
    assert_eq!(security.login("u1", "mypassword"), Some("u1".to_string()));
    assert_eq!(security.login("u1", "invalidpassword"), None);

    // Check plain password authentication
    assert_eq!(security.login("u3", "mypassword"), Some("u3".to_string()));
    assert_eq!(security.login("u3", "invalidpassword"), None);

    // Check client cert authentication
    assert!(security.login_cert("u2"));
    assert!(!security.login_cert("u1"));

    std::fs::remove_file(&temp_file).ok();
}

#[test]
fn test_check_errors() {
    // Test valid names
    assert!(Security::is_valid_group_name("@test"));
    assert!(!Security::is_valid_group_name("test"));

    assert!(Security::is_valid_user_name("test"));
    assert!(!Security::is_valid_user_name("@test"));

    // Group references non-existing user
    let json_invalid = r##"{
        "group": [
            {
                "name": "@g1",
                "members": ["u1", "u2"]
            }
        ]
    }"##;
    let temp_file = std::env::temp_dir().join("test_auth_invalid.json");
    std::fs::write(&temp_file, json_invalid).unwrap();
    assert!(Security::load_json(temp_file.to_str().unwrap()).is_err());
    std::fs::remove_file(&temp_file).ok();

    // Invalid username
    let json_invalid = r##"{
        "authentication": [
            {
                "name": "@u1",
                "method": "anonymous"
            }
        ]
    }"##;
    std::fs::write(&temp_file, json_invalid).unwrap();
    assert!(Security::load_json(temp_file.to_str().unwrap()).is_err());
    std::fs::remove_file(&temp_file).ok();

    // Invalid group name
    let json_invalid = r##"{
        "group": [
            {
                "name": "g1",
                "members": ["u1", "u2"]
            }
        ]
    }"##;
    std::fs::write(&temp_file, json_invalid).unwrap();
    assert!(Security::load_json(temp_file.to_str().unwrap()).is_err());
    std::fs::remove_file(&temp_file).ok();
}

#[test]
fn test_check_publish() {
    let json_content = r##"{
        "authentication": [
            {
                "name": "u1",
                "method": "sha256",
                "salt": "salt",
                "digest": "mypassword"
            },
            {
                "name": "u2",
                "method": "client_cert"
            },
            {
                "name": "anonymous",
                "method": "anonymous"
            }
        ],
        "group": [
            {
                "name": "@g1",
                "members": ["u1", "u2"]
            }
        ],
        "authorization": [
            {
                "topic": "#",
                "deny": {
                    "sub": ["@g1"],
                    "pub": ["@g1"]
                }
            },
            {
                "topic": "sub/#",
                "allow": {
                    "sub": ["@g1"],
                    "pub": ["@g1"]
                }
            },
            {
                "topic": "sub/topic1",
                "deny": {
                    "sub": ["u1", "anonymous"],
                    "pub": ["u1", "anonymous"]
                }
            }
        ]
    }"##;

    let temp_file = std::env::temp_dir().join("test_auth_publish.json");
    std::fs::write(&temp_file, json_content).unwrap();
    let security = Security::load_json(temp_file.to_str().unwrap()).unwrap();

    assert_eq!(security.auth_pub("topic", "u1"), AuthorizationType::Deny);
    assert_eq!(
        security.auth_pub("sub/topic", "u1"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_pub("sub/topic1", "u1"),
        AuthorizationType::Deny
    );

    assert_eq!(security.auth_sub("topic", "u1"), AuthorizationType::Deny);
    assert_eq!(
        security.auth_sub("sub/topic", "u1"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_sub("sub/topic1", "u1"),
        AuthorizationType::Deny
    );

    std::fs::remove_file(&temp_file).ok();
}

#[test]
fn test_check_publish_any() {
    let json_content = r##"{
        "authentication": [
            {
                "name": "u1",
                "method": "sha256",
                "salt": "salt",
                "digest": "mypassword"
            },
            {
                "name": "u2",
                "method": "client_cert"
            },
            {
                "name": "anonymous",
                "method": "anonymous"
            }
        ],
        "authorization": [
            {
                "topic": "#",
                "deny": {
                    "sub": ["@any"],
                    "pub": ["@any"]
                }
            },
            {
                "topic": "sub/#",
                "allow": {
                    "sub": ["@any"],
                    "pub": ["@any"]
                }
            },
            {
                "topic": "sub/topic1",
                "deny": {
                    "sub": ["u1", "anonymous"],
                    "pub": ["u1", "anonymous"]
                }
            }
        ]
    }"##;

    let temp_file = std::env::temp_dir().join("test_auth_publish_any.json");
    std::fs::write(&temp_file, json_content).unwrap();
    let security = Security::load_json(temp_file.to_str().unwrap()).unwrap();

    assert_eq!(security.auth_pub("topic", "u1"), AuthorizationType::Deny);
    assert_eq!(
        security.auth_pub("sub/topic", "u1"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_pub("sub/topic1", "u1"),
        AuthorizationType::Deny
    );

    assert_eq!(security.auth_sub("topic", "u1"), AuthorizationType::Deny);
    assert_eq!(
        security.auth_sub("sub/topic", "u1"),
        AuthorizationType::Allow
    );
    assert_eq!(
        security.auth_sub("sub/topic1", "u1"),
        AuthorizationType::Deny
    );

    std::fs::remove_file(&temp_file).ok();
}

#[test]
fn test_sha256hash() {
    let hash = Security::sha256hash("password");
    assert_eq!(hash.len(), 64); // SHA256 produces 64 hex characters

    // Verify a known hash
    let hash = Security::sha256hash("saltmypassword");
    // Expected: 38ea2e5e88fcd692fe177c6cada15e9b2db6e70bee0a0d6678c8d3b2a9aae2ad
    assert_eq!(
        hash.to_lowercase(),
        "38ea2e5e88fcd692fe177c6cada15e9b2db6e70bee0a0d6678c8d3b2a9aae2ad"
    );
}

#[test]
fn test_authorized_check() {
    // Exact match
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("example/value/a"),
            &Security::get_topic_filter_tokens("example/value/a")
        ),
        Some("example/value/a".to_string())
    );

    // Single-level wildcard in authorized filter
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("example/+/a"),
            &Security::get_topic_filter_tokens("example/value/a")
        ),
        Some("example/value/a".to_string())
    );

    // Mismatch
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("example/+/b"),
            &Security::get_topic_filter_tokens("example/value/a")
        ),
        None
    );

    // Single-level wildcard in subscription filter
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("example/value/a"),
            &Security::get_topic_filter_tokens("example/+/a")
        ),
        Some("example/value/a".to_string())
    );

    // Multi-level wildcard in authorized filter
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("example/#"),
            &Security::get_topic_filter_tokens("example/value/a")
        ),
        Some("example/value/a".to_string())
    );

    // Multi-level wildcard in subscription filter
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("example/value/a"),
            &Security::get_topic_filter_tokens("example/#")
        ),
        Some("example/value/a".to_string())
    );

    // Mismatch
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("example/deny"),
            &Security::get_topic_filter_tokens("example/test")
        ),
        None
    );

    // Single level tests
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+"),
            &Security::get_topic_filter_tokens("t1")
        ),
        Some("t1".to_string())
    );

    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+"),
            &Security::get_topic_filter_tokens("t1/")
        ),
        None
    );

    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+"),
            &Security::get_topic_filter_tokens("t1/t2")
        ),
        None
    );

    // Two level tests
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+/+"),
            &Security::get_topic_filter_tokens("t1")
        ),
        None
    );

    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+/+"),
            &Security::get_topic_filter_tokens("t1/")
        ),
        Some("t1/".to_string())
    );

    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+/+"),
            &Security::get_topic_filter_tokens("t1/t2")
        ),
        Some("t1/t2".to_string())
    );

    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+/+"),
            &Security::get_topic_filter_tokens("t1/t2/t3")
        ),
        None
    );

    // Single level with trailing slash
    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+/"),
            &Security::get_topic_filter_tokens("t1")
        ),
        None
    );

    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+/"),
            &Security::get_topic_filter_tokens("t1/")
        ),
        Some("t1/".to_string())
    );

    assert_eq!(
        Security::is_subscribe_allowed(
            &Security::get_topic_filter_tokens("+/"),
            &Security::get_topic_filter_tokens("t1/t2")
        ),
        None
    );
}

#[test]
fn test_deny_check() {
    // Exact match
    assert!(Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/value/a"),
        &Security::get_topic_filter_tokens("example/value/a")
    ));

    assert!(!Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/value/a"),
        &Security::get_topic_filter_tokens("example/value/b")
    ));

    // Single-level wildcard
    assert!(Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/+/a"),
        &Security::get_topic_filter_tokens("example/value/a")
    ));

    assert!(Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/+/a"),
        &Security::get_topic_filter_tokens("example/+/a")
    ));

    assert!(!Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/value/a"),
        &Security::get_topic_filter_tokens("example/+/a")
    ));

    // Multi-level wildcard
    assert!(Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/#"),
        &Security::get_topic_filter_tokens("example/#")
    ));

    assert!(Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/#"),
        &Security::get_topic_filter_tokens("example/+")
    ));

    assert!(!Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/+"),
        &Security::get_topic_filter_tokens("example/#")
    ));

    assert!(Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/#"),
        &Security::get_topic_filter_tokens("example/value")
    ));

    assert!(!Security::is_subscribe_denied(
        &Security::get_topic_filter_tokens("example/value"),
        &Security::get_topic_filter_tokens("example/#")
    ));
}

#[test]
fn test_auth_check() {
    let json_content = r##"{
        "authentication": [
            {
                "name": "u1",
                "method": "sha256",
                "salt": "salt",
                "digest": "75c111ce6542425228c157b1187076ed86e837f6085e3bb30b976114f70abc40"
            },
            {
                "name": "u2",
                "method": "client_cert"
            },
            {
                "name": "anonymous",
                "method": "anonymous"
            }
        ],
        "group": [
            {
                "name": "@g1",
                "members": ["u1", "u2", "anonymous"]
            }
        ],
        "authorization": [
            {
                "topic": "#",
                "allow": { "pub": ["@g1"] }
            },
            {
                "topic": "#",
                "deny": { "sub": ["@g1"] }
            },
            {
                "topic": "sub/#",
                "allow": {
                    "sub": ["@g1"],
                    "pub": ["@g1"]
                }
            },
            {
                "topic": "sub/topic1",
                "deny": {
                    "sub": ["u1", "anonymous"],
                    "pub": ["u1", "anonymous"]
                }
            }
        ]
    }"##;

    let temp_file = std::env::temp_dir().join("test_auth_check.json");
    std::fs::write(&temp_file, json_content).unwrap();
    let security = Security::load_json(temp_file.to_str().unwrap()).unwrap();

    assert!(!security.get_auth_sub_topics("u1", "sub/test").is_empty());
    assert!(security.get_auth_sub_topics("u1", "sub/topic1").is_empty());
    assert!(security
        .get_auth_sub_topics("u1", "example/topic1")
        .is_empty());

    std::fs::remove_file(&temp_file).ok();
}

#[test]
fn test_priority() {
    let json_content = r##"{
        "authentication": [
            {
                "name": "u1",
                "method": "plain_password",
                "password": "hoge"
            }
        ],
        "authorization": [
            {
                "topic": "t1",
                "allow": { "pub":["u1"], "sub":["u1"] }
            },
            {
                "topic": "#",
                "deny": { "pub":["u1"], "sub":["u1"] }
            },
            {
                "topic": "t2",
                "allow": { "pub":["u1"], "sub":["u1"] }
            }
        ]
    }"##;

    let temp_file = std::env::temp_dir().join("test_auth_priority.json");
    std::fs::write(&temp_file, json_content).unwrap();
    let security = Security::load_json(temp_file.to_str().unwrap()).unwrap();

    // Later rules have higher priority, so "#" deny overrides "t1" allow
    assert_eq!(security.auth_sub("t1", "u1"), AuthorizationType::Deny);
    // But "t2" allow comes after "#" deny, so it overrides
    assert_eq!(security.auth_sub("t2", "u1"), AuthorizationType::Allow);
    // "t3" has no specific rule, so it gets denied by "#"
    assert_eq!(security.auth_sub("t3", "u1"), AuthorizationType::Deny);

    assert_eq!(security.auth_pub("t1", "u1"), AuthorizationType::Deny);
    assert_eq!(security.auth_pub("t2", "u1"), AuthorizationType::Allow);
    assert_eq!(security.auth_pub("t3", "u1"), AuthorizationType::Deny);

    std::fs::remove_file(&temp_file).ok();
}

#[test]
fn test_subscription_level_check() {
    let json_content = r##"{
        "authentication": [
            {
                "name": "u1",
                "method": "plain_password",
                "password": "hoge"
            }
        ],
        "authorization": [
            {
                "topic": "#",
                "deny": { "pub":["u1"], "sub":["u1"] }
            },
            {
                "topic": "1/#",
                "allow": { "pub":["u1"], "sub":["u1"] }
            },
            {
                "topic": "1/2/#",
                "deny": { "pub":["u1"], "sub":["u1"] }
            }
        ]
    }"##;

    let temp_file = std::env::temp_dir().join("test_auth_sublevel.json");
    std::fs::write(&temp_file, json_content).unwrap();
    let security = Security::load_json(temp_file.to_str().unwrap()).unwrap();

    assert_eq!(security.auth_sub("1/2", "u1"), AuthorizationType::Allow);
    assert_eq!(security.auth_sub("1/2/3", "u1"), AuthorizationType::Deny);
    assert_eq!(security.auth_sub("1/2/", "u1"), AuthorizationType::Deny);

    assert!(security.is_subscribe_authorized("u1", "1/2"));
    assert!(!security.is_subscribe_authorized("u1", "1/2/3"));
    assert!(!security.is_subscribe_authorized("u1", "1/2/"));

    std::fs::remove_file(&temp_file).ok();
}

#[test]
fn test_allow_partial_deny_group() {
    let json_content = r##"{
        "authentication": [
            {
                "name": "u1",
                "method": "plain_password",
                "password": "hoge"
            },
            {
                "name": "u2",
                "method": "plain_password",
                "password": "hoge"
            }
        ],
        "authorization": [
            {
                "topic": "#",
                "allow": { "pub":["u1"], "sub":["u1"] }
            },
            {
                "topic": "#",
                "deny": { "pub":["u2"], "sub":["u2"] }
            }
        ]
    }"##;

    let temp_file = std::env::temp_dir().join("test_auth_partial.json");
    std::fs::write(&temp_file, json_content).unwrap();
    let security = Security::load_json(temp_file.to_str().unwrap()).unwrap();

    // sub
    assert!(security.is_subscribe_authorized("u1", "topic"));
    assert!(!security.is_subscribe_authorized("u2", "topic"));

    // pub
    assert_eq!(security.auth_pub("topic", "u1"), AuthorizationType::Allow);
    assert_eq!(security.auth_pub("topic", "u2"), AuthorizationType::Deny);

    // deliver
    assert_eq!(security.auth_sub("topic", "u1"), AuthorizationType::Allow);
    assert_eq!(security.auth_sub("topic", "u2"), AuthorizationType::Deny);

    std::fs::remove_file(&temp_file).ok();
}
