//! Runs the language-neutral vectors in ../conformance.

use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use watch_together::{ControlTracker, SseParser, OPS};

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../conformance");

fn vectors(dir: &str) -> Vec<(String, Value)> {
    let mut out: Vec<(String, Value)> = fs::read_dir(Path::new(ROOT).join(dir))
        .expect("vector dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |x| x == "json"))
        .map(|p| (p.file_name().unwrap().to_string_lossy().to_string(), serde_json::from_str(&fs::read_to_string(&p).unwrap()).unwrap()))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[test]
fn control_vectors() {
    let vs = vectors("control");
    assert!(vs.len() >= 7, "only {} control vectors", vs.len());
    for (name, v) in vs {
        let uses: Vec<String> = v["uses"].as_array().map(|a| a.iter().map(|x| x.as_str().unwrap().to_string()).collect()).unwrap_or_default();
        let mut t = ControlTracker::new(uses);
        let mut last = f64::NEG_INFINITY;
        for (i, s) in v["steps"].as_array().unwrap().iter().enumerate() {
            let at = s["at"].as_f64().unwrap();
            let w = format!("{} step {}", name, i);
            assert!(at >= last, "{}: steps out of order", w);
            last = at;
            if let Some(c) = s.get("control") {
                t.apply(c, at);
            }
            if s.get("unsupported").and_then(|u| u.as_bool()) == Some(true) {
                t.set_unsupported();
            }
            let e = match s.get("expect") {
                Some(e) => e,
                None => continue,
            };
            t.advance(at);
            if let Some(k) = e.get("known") {
                assert_eq!(json!(t.known()), *k, "{} known", w);
            }
            if let Some(lv) = e.get("level").and_then(|x| x.as_object()) {
                for (ex, want) in lv {
                    assert_eq!(json!(t.level(ex, at)), *want, "{} level {}", w, ex);
                }
            }
            if let Some(fz) = e.get("frozen").and_then(|x| x.as_object()) {
                for (ex, want) in fz {
                    assert_eq!(json!(t.is_frozen(ex, at)), *want, "{} frozen {}", w, ex);
                }
            }
            if let Some(want) = e.get("notice") {
                let got = match t.notice() {
                    Some(n) => json!({"kind": n.kind, "inMs": n.in_ms, "why": n.why}),
                    None => Value::Null,
                };
                assert_eq!(got, *want, "{} notice", w);
            }
            if let Some(want) = e.get("acks") {
                let got: Vec<Value> = t.drain_acks().into_iter().map(|a| json!({"epoch": a.epoch, "ex": a.ex, "level": a.level})).collect();
                assert_eq!(Value::Array(got), *want, "{} acks", w);
            }
        }
    }
}

#[test]
fn sse_vectors() {
    for (name, v) in vectors("sse") {
        let mut p = SseParser::new();
        let mut got = Vec::new();
        for c in v["chunks"].as_array().unwrap() {
            for e in p.feed(c.as_str().unwrap()) {
                got.push(json!({"event": e.event, "id": e.id, "data": e.data}));
            }
        }
        assert_eq!(Value::Array(got), v["events"], "{}", name);
    }
}

#[test]
fn ops() {
    let j: Value = serde_json::from_str(&fs::read_to_string(Path::new(ROOT).join("ops.json")).unwrap()).unwrap();
    assert_eq!(json!(OPS), j["ops"]);
}
