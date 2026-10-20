// Package watchtogether is the WATCH TOGETHER connector for Go (standard library only).
//
// One client, two transports: WebSocket (P2P daemon) and REST + Server-Sent Events (agent).
// Read-only: there is no order, key or balance operation in this API.
package watchtogether

import (
	"math"
	"strings"
	"sync"
)

// Any is the exchange key that applies to every exchange.
const Any = "*"

var rank = map[string]int{"free": 0, "soft": 1, "hard": 2}

func norm(ex string) string {
	s := strings.ToLower(strings.TrimSpace(ex))
	if s == "" {
		return Any
	}
	return s
}

func num(v any) float64 {
	f, ok := v.(float64)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

// Ack is one acknowledgement the tracker produced (a change of level for a used exchange).
type Ack struct {
	Epoch int64
	Ex    string
	Level string
}

// Notice is a planned restart or stop.
type Notice struct {
	Kind string
	InMs int64
	Why  *string
}

type entry struct {
	level string
	until float64
	why   []string
}

type frozenEntry struct {
	until float64
	why   *string
}

// ControlTracker turns control states into a per-exchange level on the caller's monotonic clock (ms). Pure; safe for concurrent use.
// Spec: ../docs/control-plane.md · vectors: ../conformance/control/*.json
type ControlTracker struct {
	mu          sync.Mutex
	known       bool
	unsupported bool
	boot        *string
	epoch       int64
	table       map[string]entry
	frozen      map[string]frozenEntry
	notice      *Notice
	order       []string
	prev        map[string]string
	acks        []Ack
}

// NewControlTracker — uses: exchanges this program calls itself (only these are acknowledged).
func NewControlTracker(uses []string) *ControlTracker {
	t := &ControlTracker{epoch: -1, table: map[string]entry{}, frozen: map[string]frozenEntry{}, prev: map[string]string{}}
	for _, ex := range uses {
		k := norm(ex)
		if _, ok := t.prev[k]; !ok {
			t.order = append(t.order, k)
			t.prev[k] = "free"
		}
	}
	return t
}

func (t *ControlTracker) levelLocked(ex string, at float64) string {
	if !t.known {
		return "unknown"
	}
	best := "free"
	for _, k := range []string{norm(ex), Any} {
		if e, ok := t.table[k]; ok && at < e.until && rank[e.level] > rank[best] {
			best = e.level
		}
	}
	return best
}

// Level is "free", "soft", "hard" or "unknown".
func (t *ControlTracker) Level(ex string, at float64) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.levelLocked(ex, at)
}

// IsFrozen reports whether the agent is backing off this exchange at `at`.
func (t *ControlTracker) IsFrozen(ex string, at float64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	f, ok := t.frozen[norm(ex)]
	return ok && at < f.until
}

// Why lists the causes in effect for this exchange.
func (t *ControlTracker) Why(ex string, at float64) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := []string{}
	for _, k := range []string{norm(ex), Any} {
		if e, ok := t.table[k]; ok && at < e.until {
			for _, w := range e.why {
				dup := false
				for _, x := range out {
					if x == w {
						dup = true
					}
				}
				if !dup {
					out = append(out, w)
				}
			}
		}
	}
	return out
}

// NextChangeAt is the earliest time after `at` when a level or frozen flag can change without a new message (ok=false: none).
func (t *ControlTracker) NextChangeAt(at float64) (float64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := math.Inf(1)
	for _, e := range t.table {
		if e.until > at && e.until < n {
			n = e.until
		}
	}
	for _, f := range t.frozen {
		if f.until > at && f.until < n {
			n = f.until
		}
	}
	return n, !math.IsInf(n, 1)
}

func (t *ControlTracker) advanceLocked(at float64) {
	if !t.known {
		return
	}
	for _, ex := range t.order {
		l := t.levelLocked(ex, at)
		if l != t.prev[ex] {
			t.prev[ex] = l
			t.acks = append(t.acks, Ack{Epoch: t.epoch, Ex: ex, Level: l})
		}
	}
}

// Advance moves the tracker to `at` (leases can expire without a message).
func (t *ControlTracker) Advance(at float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.advanceLocked(at)
}

// Apply a full control state (decoded JSON object) received at `at`. False when ignored (older epoch in the same boot).
func (t *ControlTracker) Apply(c map[string]any, at float64) bool {
	if c == nil {
		return false
	}
	ef, ok := c["epoch"].(float64)
	if !ok || math.IsNaN(ef) || math.IsInf(ef, 0) {
		return false
	}
	e := int64(ef)
	var b *string
	if v, ok := c["boot"]; ok && v != nil {
		s := toString(v)
		b = &s
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.known && eqStr(b, t.boot) && e < t.epoch {
		return false
	}
	t.boot, t.epoch, t.known, t.unsupported = b, e, true, false
	t.table = map[string]entry{}
	if y, ok := c["yield"].(map[string]any); ok {
		for ex, raw := range y {
			v, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			lv, _ := v["level"].(string)
			if lv != "soft" && lv != "hard" {
				continue
			}
			why := []string{}
			if w, ok := v["why"].([]any); ok {
				for _, x := range w {
					why = append(why, toString(x))
				}
			}
			t.table[norm(ex)] = entry{level: lv, until: at + math.Max(0, num(v["ttlMs"])), why: why}
		}
	}
	t.frozen = map[string]frozenEntry{}
	if fz, ok := c["frozen"].(map[string]any); ok {
		for ex, raw := range fz {
			v, ok := raw.(map[string]any)
			if !ok || num(v["untilMs"]) <= 0 {
				continue
			}
			var w *string
			if v["why"] != nil {
				s := toString(v["why"])
				w = &s
			}
			t.frozen[norm(ex)] = frozenEntry{until: at + num(v["untilMs"]), why: w}
		}
	}
	t.notice = nil
	if n, ok := c["notice"].(map[string]any); ok {
		if k, ok := n["kind"].(string); ok && k != "" {
			var w *string
			if n["why"] != nil {
				s := toString(n["why"])
				w = &s
			}
			t.notice = &Notice{Kind: k, InMs: int64(num(n["inMs"])), Why: w}
		}
	}
	t.advanceLocked(at)
	return true
}

// SetUnsupported: the agent has no control plane (control.state answered 501/404). Levels stay "unknown" — never "free".
func (t *ControlTracker) SetUnsupported() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.unsupported, t.known = true, false
}

// DrainAcks returns and clears the acknowledgements produced so far.
func (t *ControlTracker) DrainAcks() []Ack {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.acks
	t.acks = nil
	return a
}

// Known reports whether a control state has been applied.
func (t *ControlTracker) Known() bool { t.mu.Lock(); defer t.mu.Unlock(); return t.known }

// Unsupported reports whether the agent answered 501/404 (no control plane).
func (t *ControlTracker) Unsupported() bool { t.mu.Lock(); defer t.mu.Unlock(); return t.unsupported }

// Notice is the last notice (nil when none).
func (t *ControlTracker) Notice() *Notice { t.mu.Lock(); defer t.mu.Unlock(); return t.notice }

func eqStr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
