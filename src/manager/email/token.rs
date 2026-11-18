//! Signed one-click unsubscribe tokens.
//!
//! `v1.<base64url(user|category|expires)>.<base64url(hmac-sha256)>`. The signature covers the
//! payload, so a token can only switch off the email of one category for one user.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Unsubscribe links stay valid for a year (people click old emails).
pub const TOKEN_TTL_SECS: i64 = 365 * 24 * 3600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeClaim {
    pub user_id: String,
    pub category: String,
}

fn mac(secret: &str, payload: &[u8]) -> HmacSha256 {
    let mut m = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts any key size");
    m.update(payload);
    m
}

pub fn sign(secret: &str, user_id: &str, category: &str, now: i64) -> String {
    let payload = format!("{user_id}|{category}|{}", now + TOKEN_TTL_SECS);
    let sig = mac(secret, payload.as_bytes()).finalize().into_bytes();
    format!(
        "v1.{}.{}",
        URL_SAFE_NO_PAD.encode(payload),
        URL_SAFE_NO_PAD.encode(sig)
    )
}

/// `None` for anything malformed, forged or expired.
pub fn verify(secret: &str, token: &str, now: i64) -> Option<UnsubscribeClaim> {
    if secret.is_empty() {
        return None;
    }
    let mut parts = token.trim().split('.');
    if parts.next()? != "v1" {
        return None;
    }
    let payload = URL_SAFE_NO_PAD.decode(parts.next()?).ok()?;
    let sig = URL_SAFE_NO_PAD.decode(parts.next()?).ok()?;
    if parts.next().is_some() {
        return None;
    }
    mac(secret, &payload).verify_slice(&sig).ok()?;
    let text = String::from_utf8(payload).ok()?;
    let mut f = text.split('|');
    let (user_id, category, exp) = (f.next()?, f.next()?, f.next()?);
    if f.next().is_some() || user_id.is_empty() || category.is_empty() {
        return None;
    }
    if exp.parse::<i64>().ok()? < now {
        return None;
    }
    Some(UnsubscribeClaim {
        user_id: user_id.to_string(),
        category: category.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let t = sign("k", "u1", "reminder", 1000);
        assert_eq!(
            verify("k", &t, 2000),
            Some(UnsubscribeClaim {
                user_id: "u1".into(),
                category: "reminder".into()
            })
        );
    }

    #[test]
    fn rejects_forged_expired_and_garbage() {
        let t = sign("k", "u1", "reminder", 1000);
        assert!(verify("other", &t, 2000).is_none());
        assert!(verify("k", &t, 1000 + TOKEN_TTL_SECS + 1).is_none());
        assert!(verify("", &t, 2000).is_none());
        assert!(verify("k", "garbage", 2000).is_none());
        assert!(verify("k", "v1.a.b.c", 2000).is_none());
        // Swapping the payload keeps the old signature: must fail.
        let other = sign("k", "u2", "reminder", 1000);
        let forged = format!(
            "v1.{}.{}",
            other.split('.').nth(1).unwrap(),
            t.split('.').nth(2).unwrap()
        );
        assert!(verify("k", &forged, 2000).is_none());
    }
}
