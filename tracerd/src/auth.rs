use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use sha2::{Digest, Sha256};

pub const SERVICE_HEADER: &str = "x-aetelier-service";
pub const SERVICE_TOKEN_ENV: &str = "TRACER_SERVICE_TOKEN";
pub const AUTH_MODE_ENV: &str = "TRACER_AUTH";
pub const MIN_TOKEN_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Required,
    Optional,
    Disabled,
}

impl std::str::FromStr for AuthMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.trim() {
            "required" => Ok(Self::Required),
            "optional" => Ok(Self::Optional),
            "disabled" => Ok(Self::Disabled),
            other => {
                anyhow::bail!("{AUTH_MODE_ENV}={other}: expected required, optional or disabled")
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceCheck {
    Absent,
    Valid,
    Invalid,
}

#[derive(Debug)]
pub struct ServiceAuth {
    mode: AuthMode,
    digest: Option<[u8; 32]>,
}

impl ServiceAuth {
    pub fn new(mode: AuthMode, token: Option<&str>) -> anyhow::Result<Self> {
        if mode == AuthMode::Disabled {
            return Ok(Self { mode, digest: None });
        }
        let digest = match token.filter(|t| !t.is_empty()) {
            Some(t) => {
                anyhow::ensure!(
                    t.len() >= MIN_TOKEN_BYTES,
                    "{SERVICE_TOKEN_ENV} must be at least {MIN_TOKEN_BYTES} bytes"
                );
                Some(digest_of(t))
            }
            None => {
                anyhow::ensure!(
                    mode != AuthMode::Required,
                    "{AUTH_MODE_ENV}=required needs {SERVICE_TOKEN_ENV}"
                );
                None
            }
        };
        Ok(Self { mode, digest })
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let mode = match std::env::var(AUTH_MODE_ENV) {
            Ok(v) if !v.trim().is_empty() => v.parse()?,
            _ => AuthMode::Required,
        };
        let token = std::env::var(SERVICE_TOKEN_ENV).ok();
        Self::new(mode, token.as_deref())
    }

    pub fn shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    pub fn check(&self, headers: &HeaderMap) -> ServiceCheck {
        if self.mode == AuthMode::Disabled {
            return ServiceCheck::Absent;
        }
        let Some(presented) = headers.get(SERVICE_HEADER) else {
            return ServiceCheck::Absent;
        };
        let Some(expected) = &self.digest else {
            return ServiceCheck::Invalid;
        };
        let matches = presented
            .to_str()
            .is_ok_and(|p| constant_time_eq(&digest_of(p), expected));
        if matches {
            ServiceCheck::Valid
        } else {
            ServiceCheck::Invalid
        }
    }

    pub fn admits(&self, headers: &HeaderMap) -> bool {
        match self.check(headers) {
            ServiceCheck::Valid => true,
            ServiceCheck::Invalid => false,
            ServiceCheck::Absent => self.mode != AuthMode::Required,
        }
    }
}

fn digest_of(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub async fn require_service(
    State(auth): State<Arc<ServiceAuth>>,
    request: Request,
    next: Next,
) -> Response {
    if auth.admits(request.headers()) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "service token required"})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    fn headers(token: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(t) = token {
            h.insert(SERVICE_HEADER, t.parse().unwrap());
        }
        h
    }

    #[test]
    fn modes_parse_and_reject_unknown_values() {
        assert_eq!("required".parse::<AuthMode>().unwrap(), AuthMode::Required);
        assert_eq!(
            " optional ".parse::<AuthMode>().unwrap(),
            AuthMode::Optional
        );
        assert_eq!("disabled".parse::<AuthMode>().unwrap(), AuthMode::Disabled);
        assert!("on".parse::<AuthMode>().is_err());
    }

    #[test]
    fn required_mode_needs_a_long_enough_token() {
        assert!(ServiceAuth::new(AuthMode::Required, None).is_err());
        assert!(ServiceAuth::new(AuthMode::Required, Some("")).is_err());
        assert!(ServiceAuth::new(AuthMode::Required, Some("short")).is_err());
        assert!(ServiceAuth::new(AuthMode::Optional, Some("short")).is_err());
        assert!(ServiceAuth::new(AuthMode::Required, Some(TOKEN)).is_ok());
        assert!(ServiceAuth::new(AuthMode::Optional, None).is_ok());
        assert!(ServiceAuth::new(AuthMode::Disabled, Some("short")).is_ok());
    }

    #[test]
    fn checks_follow_the_mode() {
        let required = ServiceAuth::new(AuthMode::Required, Some(TOKEN)).unwrap();
        assert_eq!(required.check(&headers(Some(TOKEN))), ServiceCheck::Valid);
        assert_eq!(
            required.check(&headers(Some("wrong"))),
            ServiceCheck::Invalid
        );
        assert_eq!(required.check(&headers(None)), ServiceCheck::Absent);
        assert!(required.admits(&headers(Some(TOKEN))));
        assert!(!required.admits(&headers(None)));
        assert!(!required.admits(&headers(Some("wrong"))));

        let optional = ServiceAuth::new(AuthMode::Optional, Some(TOKEN)).unwrap();
        assert!(optional.admits(&headers(None)));
        assert!(!optional.admits(&headers(Some("wrong"))));

        let tokenless = ServiceAuth::new(AuthMode::Optional, None).unwrap();
        assert_eq!(
            tokenless.check(&headers(Some(TOKEN))),
            ServiceCheck::Invalid
        );

        let disabled = ServiceAuth::new(AuthMode::Disabled, None).unwrap();
        assert_eq!(disabled.check(&headers(Some(TOKEN))), ServiceCheck::Absent);
        assert!(disabled.admits(&headers(None)));
    }
}
