//! Server-Sent Events parser — pure, incremental. Vectors: ../../conformance/sse/*.json

/// One event. `id` is None when the block had no id field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: String,
    pub id: Option<String>,
    pub data: String,
}

#[derive(Default)]
pub struct SseParser {
    buf: String,
    data: Vec<String>,
    event: Option<String>,
    id: Option<String>,
    has: bool,
    pub last_event_id: Option<String>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a chunk; returns the events it completed.
    pub fn feed(&mut self, chunk: &str) -> Vec<SseEvent> {
        self.buf.push_str(chunk);
        let mut out = Vec::new();
        loop {
            let i = match self.buf.find(|c: char| c == '\r' || c == '\n') {
                Some(i) => i,
                None => break,
            };
            let bytes = self.buf.as_bytes();
            let n = if bytes[i] == b'\r' {
                if i == bytes.len() - 1 {
                    break; // may be the first half of \r\n — wait for more
                }
                if bytes[i + 1] == b'\n' {
                    2
                } else {
                    1
                }
            } else {
                1
            };
            let line = self.buf[..i].to_string();
            self.buf.replace_range(..i + n, "");
            self.line(&line, &mut out);
        }
        out
    }

    fn line(&mut self, l: &str, out: &mut Vec<SseEvent>) {
        if l.is_empty() {
            if self.has {
                out.push(SseEvent {
                    event: self.event.take().unwrap_or_else(|| "message".to_string()),
                    id: self.id.take(),
                    data: self.data.join("\n"),
                });
            }
            self.data.clear();
            self.event = None;
            self.id = None;
            self.has = false;
            return;
        }
        if l.starts_with(':') {
            return;
        }
        let (field, value) = match l.find(':') {
            Some(i) => (&l[..i], &l[i + 1..]),
            None => (l, ""),
        };
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => {
                self.data.push(value.to_string());
                self.has = true;
            }
            "event" => self.event = Some(value.to_string()),
            "id" => {
                self.id = Some(value.to_string());
                self.last_event_id = Some(value.to_string());
            }
            _ => {}
        }
    }
}
