//! Control-plane tracker — pure. Turns control states into a per-exchange level on the caller's monotonic clock (ms).
//! Spec: ../../docs/control-plane.md · vectors: ../../conformance/control/*.json

use serde_json::Value;
use std::collections::HashMap;

/// The exchange key that applies to every exchange.
pub const ANY: &str = "*";

fn rank(l: &str) -> i32 {
    match l {
        "soft" => 1,
        "hard" => 2,
        _ => 0,
    }
}

pub(crate) fn norm(ex: &str) -> String {
    let s = ex.trim().to_lowercase();
    if s.is_empty() {
        ANY.to_string()
    } else {
        s
    }
}

fn num(v: Option<&Value>) -> f64 {
    v.and_then(|x| x.as_f64()).filter(|f| f.is_finite()).unwrap_or(0.0)
}

fn str_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn opt_str(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(x) => Some(str_of(x)),
    }
}

/// An acknowledgement the tracker produced (a change of level for a used exchange).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    pub epoch: i64,
    pub ex: String,
    pub level: String,
}

/// A planned restart or stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub kind: String,
    pub in_ms: i64,
    pub why: Option<String>,
}

struct Entry {
    level: &'static str,
    until: f64,
    why: Vec<String>,
}

struct Frozen {
    until: f64,
    #[allow(dead_code)]
    why: Option<String>,
}

/// See the module docs.
pub struct ControlTracker {
    known: bool,
    unsupported: bool,
    boot: Option<String>,
    epoch: i64,
    table: HashMap<String, Entry>,
    frozen: HashMap<String, Frozen>,
    notice: Option<Notice>,
    order: Vec<String>,
    prev: HashMap<String, String>,
    acks: Vec<Ack>,
}

impl ControlTracker {
    /// `uses`: exchanges this program calls itself — only these are acknowledged.
    pub fn new<I, S>(uses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut t = ControlTracker {
            known: false,
            unsupported: false,
            boot: None,
            epoch: -1,
            table: HashMap::new(),
            frozen: HashMap::new(),
            notice: None,
            order: Vec::new(),
            prev: HashMap::new(),
            acks: Vec::new(),
        };
        for ex in uses {
            let k = norm(ex.as_ref());
            if !t.prev.contains_key(&k) {
                t.order.push(k.clone());
                t.prev.insert(k, "free".to_string());
            }
        }
        t
    }

    /// "free" | "soft" | "hard" | "unknown"
    pub fn level(&self, ex: &str, at: f64) -> &'static str {
        if !self.known {
            return "unknown";
        }
        let mut best = "free";
        for k in [norm(ex), ANY.to_string()] {
            if let Some(e) = self.table.get(&k) {
                if at < e.until && rank(e.level) > rank(best) {
                    best = e.level;
                }
            }
        }
        best
    }

    pub fn is_frozen(&self, ex: &str, at: f64) -> bool {
        self.frozen.get(&norm(ex)).map_or(false, |f| at < f.until)
    }

    pub fn why(&self, ex: &str, at: f64) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for k in [norm(ex), ANY.to_string()] {
            if let Some(e) = self.table.get(&k) {
                if at < e.until {
                    for w in &e.why {
                        if !out.contains(w) {
                            out.push(w.clone());
                        }
                    }
                }
            }
        }
        out
    }

    /// Earliest time after `at` when a level or frozen flag can change without a new message.
    pub fn next_change_at(&self, at: f64) -> Option<f64> {
        self.table
            .values()
            .map(|e| e.until)
            .chain(self.frozen.values().map(|f| f.until))
            .filter(|u| *u > at)
            .fold(None, |m: Option<f64>, u| Some(match m {
                Some(x) if x < u => x,
                _ => u,
            }))
    }

    /// Move to `at` (leases can expire without a message).
    pub fn advance(&mut self, at: f64) {
        if !self.known {
            return;
        }
        for i in 0..self.order.len() {
            let ex = self.order[i].clone();
            let l = self.level(&ex, at);
            if self.prev.get(&ex).map(|s| s.as_str()) != Some(l) {
                self.prev.insert(ex.clone(), l.to_string());
                self.acks.push(Ack { epoch: self.epoch, ex, level: l.to_string() });
            }
        }
    }

    /// Apply a full control state received at `at`. False when ignored (older epoch in the same boot).
    pub fn apply(&mut self, c: &Value, at: f64) -> bool {
        let obj = match c.as_object() {
            Some(o) => o,
            None => return false,
        };
        let ef = match obj.get("epoch").and_then(|v| v.as_f64()) {
            Some(f) if f.is_finite() => f,
            _ => return false,
        };
        let e = ef as i64;
        let b = opt_str(obj.get("boot"));
        if self.known && b == self.boot && e < self.epoch {
            return false;
        }
        self.boot = b;
        self.epoch = e;
        self.known = true;
        self.unsupported = false;
        self.table.clear();
        if let Some(y) = obj.get("yield").and_then(|v| v.as_object()) {
            for (ex, raw) in y {
                let v = match raw.as_object() {
                    Some(v) => v,
                    None => continue,
                };
                let level: &'static str = match v.get("level").and_then(|l| l.as_str()) {
                    Some("soft") => "soft",
                    Some("hard") => "hard",
                    _ => continue,
                };
                let why: Vec<String> = v.get("why").and_then(|w| w.as_array()).map(|a| a.iter().map(str_of).collect()).unwrap_or_default();
                self.table.insert(norm(ex), Entry { level, until: at + num(v.get("ttlMs")).max(0.0), why });
            }
        }
        self.frozen.clear();
        if let Some(fz) = obj.get("frozen").and_then(|v| v.as_object()) {
            for (ex, raw) in fz {
                if let Some(v) = raw.as_object() {
                    let u = num(v.get("untilMs"));
                    if u > 0.0 {
                        self.frozen.insert(norm(ex), Frozen { until: at + u, why: opt_str(v.get("why")) });
                    }
                }
            }
        }
        self.notice = obj.get("notice").and_then(|n| n.as_object()).and_then(|n| {
            let kind = n.get("kind").and_then(|k| k.as_str()).filter(|k| !k.is_empty())?;
            Some(Notice { kind: kind.to_string(), in_ms: num(n.get("inMs")) as i64, why: opt_str(n.get("why")) })
        });
        self.advance(at);
        true
    }

    /// The agent has no control plane (control.state answered 501/404). Levels stay "unknown" — never "free".
    pub fn set_unsupported(&mut self) {
        self.unsupported = true;
        self.known = false;
    }

    pub fn drain_acks(&mut self) -> Vec<Ack> {
        std::mem::take(&mut self.acks)
    }

    pub fn known(&self) -> bool {
        self.known
    }
    pub fn unsupported(&self) -> bool {
        self.unsupported
    }
    pub fn notice(&self) -> Option<&Notice> {
        self.notice.as_ref()
    }
    pub fn epoch(&self) -> Option<i64> {
        if self.known {
            Some(self.epoch)
        } else {
            None
        }
    }
    pub fn boot(&self) -> Option<&str> {
        self.boot.as_deref()
    }
}
