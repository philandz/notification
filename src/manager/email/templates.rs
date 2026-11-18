//! Server-side email rendering (vi / en). Only the types worth an email have their own copy;
//! anything else falls back to the plain `fallback_text` the producer sent.

use serde_json::Value;

pub struct Rendered {
    pub subject: String,
    pub html: String,
    pub text: String,
}

pub struct Context<'a> {
    pub locale: &'a str,
    pub display_name: &'a str,
    pub event_type: &'a str,
    pub params: &'a Value,
    pub fallback_text: &'a str,
    /// Absolute URL of the page the notification points to, if any.
    pub link_url: Option<&'a str>,
    pub settings_url: &'a str,
    pub unsubscribe_url: &'a str,
    pub support_email: &'a str,
}

fn is_vi(locale: &str) -> bool {
    locale == "vi"
}

pub fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// `1234567` -> `1.234.567` (vi) or `1,234,567` (en).
pub fn format_amount(n: i64, vi: bool) -> String {
    let sep = if vi { '.' } else { ',' };
    let digits = n.unsigned_abs().to_string();
    let mut out = String::new();
    let mut since_sep = 0;
    for ch in digits.chars().rev() {
        if since_sep == 3 {
            out.push(sep);
            since_sep = 0;
        }
        out.push(ch);
        since_sep += 1;
    }
    let mut out: String = out.chars().rev().collect();
    if n < 0 {
        out.insert(0, '-');
    }
    out
}

fn text_param(p: &Value, key: &str) -> String {
    match p.get(key) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

fn amount_param(p: &Value, key: &str, vi: bool) -> String {
    let n = p
        .get(key)
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f.round() as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0);
    format!("{} ₫", format_amount(n, vi))
}

/// (title, body) for the notification itself.
fn copy(ctx: &Context<'_>) -> (String, String) {
    let vi = is_vi(ctx.locale);
    let p = ctx.params;
    let debt = text_param(p, "debt_name");
    match ctx.event_type {
        "debt.payment_due" => {
            let days = text_param(p, "days");
            let amount = amount_param(p, "amount", vi);
            if vi {
                (
                    format!("Khoản nợ đến hạn sau {days} ngày"),
                    format!("{debt}: khoảng {amount} (ước tính)"),
                )
            } else {
                (
                    format!("Debt payment due in {days} days"),
                    format!("{debt}: about {amount} (estimated)"),
                )
            }
        }
        "debt.payment_due_today" => {
            let amount = amount_param(p, "amount", vi);
            if vi {
                (
                    "Khoản nợ đến hạn hôm nay".into(),
                    format!("{debt}: khoảng {amount} (ước tính)"),
                )
            } else {
                (
                    "Debt payment due today".into(),
                    format!("{debt}: about {amount} (estimated)"),
                )
            }
        }
        "debt.payment_overdue" => {
            let days = text_param(p, "days");
            if vi {
                (
                    "Khoản nợ đã quá hạn".into(),
                    format!("{debt}: quá hạn {days} ngày"),
                )
            } else {
                (
                    "Debt payment overdue".into(),
                    format!("{debt}: overdue by {days} days"),
                )
            }
        }
        "debt.rate_period_ending" => {
            let date = text_param(p, "date");
            if vi {
                (
                    "Sắp đổi lãi suất".into(),
                    format!("{debt}: lãi suất thay đổi vào {date}"),
                )
            } else {
                (
                    "Interest rate period ending".into(),
                    format!("{debt}: interest rate changes on {date}"),
                )
            }
        }
        _ => {
            let title = if vi {
                "Thông báo mới từ Philandz"
            } else {
                "New notification from Philandz"
            };
            (title.into(), ctx.fallback_text.to_string())
        }
    }
}

pub fn render(ctx: &Context<'_>) -> Rendered {
    let vi = is_vi(ctx.locale);
    let (title, body) = copy(ctx);
    let name = if ctx.display_name.trim().is_empty() {
        if vi {
            "bạn".to_string()
        } else {
            "there".to_string()
        }
    } else {
        ctx.display_name.trim().to_string()
    };
    let greeting = if vi {
        format!("Xin chào {name},")
    } else {
        format!("Hi {name},")
    };
    let open = if vi {
        "Mở trong Philandz"
    } else {
        "Open in Philandz"
    };
    let why = if vi {
        "Bạn nhận email này vì đã bật thông báo qua email trong Philandz."
    } else {
        "You are receiving this because email notifications are on in Philandz."
    };
    let manage = if vi {
        "Quản lý thông báo"
    } else {
        "Manage notifications"
    };
    let unsub = if vi {
        "Ngừng nhận email loại này"
    } else {
        "Stop emails like this"
    };
    let help = if vi { "Cần hỗ trợ" } else { "Need help" };

    let button = ctx
        .link_url
        .map(|u| {
            format!(
                "<p style=\"margin:24px 0\"><a href=\"{}\" style=\"background:#0f766e;color:#fff;\
                 padding:10px 18px;border-radius:6px;text-decoration:none\">{}</a></p>",
                escape_html(u),
                open
            )
        })
        .unwrap_or_default();

    let html = format!(
        "<!doctype html><html><body style=\"font-family:Arial,Helvetica,sans-serif;color:#111;\
         max-width:560px;margin:0 auto;padding:24px\"><h2 style=\"margin:0 0 16px\">{title}</h2>\
         <p>{greeting}</p><p>{body}</p>{button}<hr style=\"border:none;border-top:1px solid #ddd;\
         margin:24px 0\"><p style=\"font-size:12px;color:#666\">{why}<br>\
         <a href=\"{settings}\">{manage}</a> · <a href=\"{unsub_url}\">{unsub}</a> · \
         {help}: <a href=\"mailto:{support}\">{support}</a></p></body></html>",
        title = escape_html(&title),
        greeting = escape_html(&greeting),
        body = escape_html(&body),
        button = button,
        why = why,
        settings = escape_html(ctx.settings_url),
        manage = manage,
        unsub_url = escape_html(ctx.unsubscribe_url),
        unsub = unsub,
        help = help,
        support = escape_html(ctx.support_email),
    );

    let mut text = format!("{greeting}\n\n{title}\n{body}\n");
    if let Some(u) = ctx.link_url {
        text.push_str(&format!("\n{open}: {u}\n"));
    }
    text.push_str(&format!(
        "\n--\n{why}\n{manage}: {}\n{unsub}: {}\n{help}: {}\n",
        ctx.settings_url, ctx.unsubscribe_url, ctx.support_email
    ));

    Rendered {
        subject: title,
        html,
        text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx<'a>(locale: &'a str, ty: &'a str, p: &'a Value) -> Context<'a> {
        Context {
            locale,
            display_name: "Lan <b>",
            event_type: ty,
            params: p,
            fallback_text: "plain fallback",
            link_url: Some("https://app.example/vi/budgets/b1"),
            settings_url: "https://app.example/vi/settings/notifications",
            unsubscribe_url: "https://api.example/unsub?token=t",
            support_email: "support@example.com",
        }
    }

    #[test]
    fn amounts_are_grouped_per_locale() {
        assert_eq!(format_amount(1234567, true), "1.234.567");
        assert_eq!(format_amount(1234567, false), "1,234,567");
        assert_eq!(format_amount(999, true), "999");
        assert_eq!(format_amount(-1500, false), "-1,500");
    }

    #[test]
    fn debt_due_in_both_languages() {
        let p = json!({"debt_name": "Vay ngân hàng", "amount": 2500000, "days": 3});
        let vi = render(&ctx("vi", "debt.payment_due", &p));
        assert_eq!(vi.subject, "Khoản nợ đến hạn sau 3 ngày");
        assert!(vi.html.contains("2.500.000 ₫"));
        assert!(vi.text.contains("Ngừng nhận email loại này"));
        let en = render(&ctx("en", "debt.payment_due", &p));
        assert_eq!(en.subject, "Debt payment due in 3 days");
        assert!(en.html.contains("2,500,000 ₫"));
    }

    #[test]
    fn html_is_escaped_and_links_present() {
        let p = json!({"debt_name": "<script>x</script>", "days": 1});
        let r = render(&ctx("en", "debt.payment_overdue", &p));
        assert!(!r.html.contains("<script>"));
        assert!(r.html.contains("Lan &lt;b&gt;"));
        assert!(r.html.contains("https://api.example/unsub?token=t"));
        assert!(r.text.contains("https://app.example/vi/budgets/b1"));
    }

    #[test]
    fn unknown_type_uses_fallback_text() {
        let p = json!({});
        let r = render(&ctx("en", "something.new", &p));
        assert_eq!(r.subject, "New notification from Philandz");
        assert!(r.text.contains("plain fallback"));
    }
}
