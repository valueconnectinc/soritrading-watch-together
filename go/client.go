package watchtogether

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ProtocolVersion of the local API.
const ProtocolVersion = 1

// Ops are the operation names of protocol v1.
var Ops = []string{"status", "conds.list", "conds.add", "conds.del", "signals.since", "peers", "usage",
	"control.state", "control.ack", "client.register", "usage.report"}

var reToken = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ApiError is an error reply. Code is machine-readable (body.error); read codes, not message text.
type ApiError struct {
	Status int
	Code   string
	Reason string
	Body   map[string]any
}

func (e *ApiError) Error() string {
	r := ""
	if e.Reason != "" {
		r = " (" + e.Reason + ")"
	}
	return fmt.Sprintf("%s%s [%d]", e.Code, r, e.Status)
}

func newErr(status int, code string, body map[string]any) *ApiError {
	e := &ApiError{Status: status, Code: code, Body: body}
	if body != nil && body["reason"] != nil {
		e.Reason = toString(body["reason"])
	}
	return e
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}

// Endpoint is a base URL and the token from the token file.
type Endpoint struct {
	URL   string
	Token string
}

// Uses declares an exchange this program calls itself. SameKey / SameHost: nil = unknown.
type Uses struct {
	Ex       string `json:"ex"`
	SameKey  *bool  `json:"sameKey"`
	SameHost *bool  `json:"sameHost"`
}

// Options for Connect.
type Options struct {
	Transport string    // "auto" (default) | "ws" | "rest"
	WS        *Endpoint // daemon WebSocket, e.g. ws://127.0.0.1:27720/v1
	REST      *Endpoint // agent REST, e.g. http://127.0.0.1:27719
	Name      string    // register under this name (control plane); empty = anonymous
	Uses      []Uses
	Reconnect bool
	Timeout   time.Duration
}

// LevelEvent is a level change for one exchange.
type LevelEvent struct {
	Ex     string
	Level  string
	Why    []string
	Frozen bool
}

type transport interface {
	kind() string
	hello() map[string]any
	request(op string, args map[string]any) (map[string]any, error)
	stream(lastSeq int64) error
	close()
}

// Client is a connection to the agent's local API. Listeners run on one internal goroutine; do not block in them.
type Client struct {
	o       Options
	Tracker *ControlTracker
	start   time.Time

	mu         sync.Mutex
	t          transport
	kindInUse  string
	clientID   string
	gate       map[string]any
	lastSeq    int64
	boot       string
	closed     bool
	lastLevels map[string]string
	levelOrder []string
	leaseTimer *time.Timer
	regTimer   *time.Timer

	lmu sync.Mutex
	ls  map[string][]*listener

	events chan func()
	done   chan struct{}
}

type listener struct{ fn func(any) }

// Connect opens a connection and syncs (register → control state → missed signals).
func Connect(o Options) (*Client, error) {
	if o.Transport == "" {
		o.Transport = "auto"
	}
	if o.Timeout == 0 {
		o.Timeout = 20 * time.Second
	}
	for _, e := range []*Endpoint{o.WS, o.REST} {
		if e != nil && !reToken.MatchString(e.Token) {
			return nil, errors.New("token must be 64 hex characters")
		}
	}
	switch {
	case o.Transport == "ws" && o.WS == nil, o.Transport == "rest" && o.REST == nil, o.Transport == "auto" && o.WS == nil && o.REST == nil:
		return nil, errors.New("missing endpoint for transport " + o.Transport)
	case o.Transport != "auto" && o.Transport != "ws" && o.Transport != "rest":
		return nil, errors.New("transport must be auto, ws or rest")
	}
	uses := make([]string, 0, len(o.Uses))
	for _, u := range o.Uses {
		uses = append(uses, u.Ex)
	}
	c := &Client{o: o, Tracker: NewControlTracker(uses), start: time.Now(), lastLevels: map[string]string{},
		ls: map[string][]*listener{}, events: make(chan func(), 256), done: make(chan struct{})}
	go c.loop()
	if err := c.open(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Client) loop() {
	for {
		select {
		case f := <-c.events:
			f()
		case <-c.done:
			return
		}
	}
}

func (c *Client) post(f func()) {
	select {
	case c.events <- f:
	case <-c.done:
	}
}

func (c *Client) now() float64 { return float64(time.Since(c.start).Nanoseconds()) / 1e6 }

// Transport is "ws" or "rest" — what is actually in use (never guessed).
func (c *Client) Transport() string { c.mu.Lock(); defer c.mu.Unlock(); return c.kindInUse }

// ClientID is the registration id ("" when anonymous or not registered).
func (c *Client) ClientID() string { c.mu.Lock(); defer c.mu.Unlock(); return c.clientID }

// On subscribes to "signal"|"control"|"gate"|"hello"|"level"|"notice"|"gap"|"transport"|"close"|"error". Returns unsubscribe.
func (c *Client) On(ev string, fn func(any)) func() {
	l := &listener{fn}
	c.lmu.Lock()
	c.ls[ev] = append(c.ls[ev], l)
	c.lmu.Unlock()
	return func() {
		c.lmu.Lock()
		defer c.lmu.Unlock()
		arr := c.ls[ev]
		for i, x := range arr {
			if x == l {
				c.ls[ev] = append(arr[:i:i], arr[i+1:]...)
				return
			}
		}
	}
}

func (c *Client) emit(ev string, v any) {
	c.lmu.Lock()
	arr := append([]*listener(nil), c.ls[ev]...)
	c.lmu.Unlock()
	for _, l := range arr {
		func() {
			defer func() { _ = recover() }() // a listener error must not break the client
			l.fn(v)
		}()
	}
}

func (c *Client) open() error {
	tryWS := c.o.Transport == "ws" || (c.o.Transport == "auto" && c.o.WS != nil)
	var t transport
	var wsErr error
	if tryWS {
		w, err := dialWS(c, *c.o.WS)
		if err != nil {
			wsErr = err
			if c.o.Transport == "ws" || c.o.REST == nil {
				return err
			}
		} else {
			t = w
		}
	}
	if t == nil {
		t = newREST(c, *c.o.REST)
	}
	c.mu.Lock()
	c.t = t
	changed := c.kindInUse != t.kind()
	c.kindInUse = t.kind()
	if h := t.hello(); h != nil {
		if g, ok := h["gate"].(map[string]any); ok {
			c.gate = g
		}
	}
	c.mu.Unlock()
	if changed {
		code := any(nil)
		var ae *ApiError
		if errors.As(wsErr, &ae) {
			code = ae.Code
		}
		ev := map[string]any{"kind": t.kind(), "wsError": code}
		c.post(func() { c.emit("transport", ev) })
	}
	c.sync()
	c.mu.Lock()
	seq := c.lastSeq
	c.mu.Unlock()
	return t.stream(seq)
}

func (c *Client) postErr(err error) { c.post(func() { c.emit("error", err) }) }

func (c *Client) sync() {
	if c.o.Name != "" {
		if err := c.register(); err != nil {
			c.postErr(err)
		}
	}
	if err := c.RefreshControl(); err != nil {
		c.postErr(err)
	}
	c.mu.Lock()
	isWS, seq := c.t.kind() == "ws", c.lastSeq
	c.mu.Unlock()
	// WebSocket has no Last-Event-ID — fetch what was missed while disconnected (REST resumes through the SSE header).
	if isWS && seq > 0 {
		r, err := c.Request("signals.since", map[string]any{"since": seq, "limit": 200})
		if err != nil {
			c.postErr(err)
			return
		}
		c.mu.Lock()
		if b, ok := r["boot"].(string); ok && b != "" {
			if c.boot != "" && b != c.boot {
				c.lastSeq = 0
			}
			c.boot = b
		}
		c.mu.Unlock()
		if gap, _ := r["gap"].(bool); gap {
			c.post(func() { c.emit("gap", map[string]any{"since": seq}) })
		}
		if items, ok := r["items"].([]any); ok {
			for _, it := range items {
				if m, ok := it.(map[string]any); ok {
					c.post(func() { c.onEvent("signal", m) })
				}
			}
		}
	}
}

func (c *Client) register() error {
	args := map[string]any{"name": c.o.Name, "uses": c.o.Uses}
	if id := c.ClientID(); id != "" {
		args["clientId"] = id
	}
	r, err := c.Request("client.register", args)
	if err != nil {
		return err
	}
	ttl := 120000.0
	if v, ok := r["ttlMs"].(float64); ok {
		ttl = v
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clientID = toString(r["clientId"])
	if c.regTimer != nil {
		c.regTimer.Stop()
	}
	if !c.closed {
		c.regTimer = time.AfterFunc(time.Duration(math.Max(5000, ttl/2))*time.Millisecond, func() {
			if err := c.register(); err != nil {
				c.postErr(err)
			}
		})
	}
	return nil
}

// RefreshControl pulls the control state (done on every connect). 501/404 = no control plane → levels stay "unknown".
func (c *Client) RefreshControl() error {
	b, err := c.Request("control.state", map[string]any{})
	if err != nil {
		var ae *ApiError
		if errors.As(err, &ae) && (ae.Status == 501 || ae.Status == 404) {
			c.Tracker.SetUnsupported()
			c.post(c.levelsChanged)
			return nil
		}
		return err
	}
	ctl, _ := b["control"].(map[string]any)
	at := c.now()
	c.post(func() { c.applyControl(ctl, at) })
	return nil
}

func (c *Client) onEvent(ev string, d map[string]any) {
	if d == nil {
		return
	}
	switch ev {
	case "signal":
		if s, ok := d["seq"].(float64); ok {
			c.mu.Lock()
			if int64(s) > c.lastSeq {
				c.lastSeq = int64(s)
			}
			c.mu.Unlock()
		}
		c.emit("signal", d)
	case "control":
		c.applyControl(d, c.now())
	case "gate":
		ok, _ := d["ok"].(bool)
		g := map[string]any{"ok": ok, "reason": d["reason"]}
		c.mu.Lock()
		c.gate = g
		c.mu.Unlock()
		c.emit("gate", g)
	case "hello":
		if g, ok := d["gate"].(map[string]any); ok {
			c.mu.Lock()
			c.gate = g
			c.mu.Unlock()
			c.emit("gate", g)
		}
		if b, ok := d["boot"].(string); ok && b != "" {
			c.mu.Lock()
			if c.boot != "" && b != c.boot {
				c.lastSeq = 0 // agent restarted — seq starts again
			}
			c.boot = b
			c.mu.Unlock()
		}
		c.emit("hello", d)
	}
}

func (c *Client) applyControl(ctl map[string]any, at float64) {
	if ctl == nil {
		return
	}
	prev := c.Tracker.Notice()
	if !c.Tracker.Apply(ctl, at) {
		return
	}
	c.emit("control", ctl)
	if n := c.Tracker.Notice(); n != nil && !noticeEq(n, prev) {
		c.emit("notice", *n)
	}
	c.levelsChanged()
}

func noticeEq(a, b *Notice) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Kind == b.Kind && a.InMs == b.InMs && eqStr(a.Why, b.Why)
}

// levelsChanged — after any change: send acks, notify guard listeners, arm the lease timer. Runs on the loop goroutine.
func (c *Client) levelsChanged() {
	at := c.now()
	c.Tracker.Advance(at)
	id := c.ClientID()
	for _, a := range c.Tracker.DrainAcks() {
		if id == "" {
			continue
		}
		args := map[string]any{"clientId": id, "epoch": a.Epoch, "ex": a.Ex, "paused": a.Level != "free"}
		go func() { _, _ = c.Request("control.ack", args) }()
	}
	for _, u := range c.o.Uses {
		k := norm(u.Ex)
		if _, ok := c.lastLevels[k]; !ok {
			c.levelOrder = append(c.levelOrder, k)
			c.lastLevels[k] = ""
		}
	}
	for _, ex := range c.levelOrder {
		l := c.Tracker.Level(ex, at)
		if c.lastLevels[ex] != l {
			c.lastLevels[ex] = l
			c.emit("level", LevelEvent{Ex: ex, Level: l, Why: c.Tracker.Why(ex, at), Frozen: c.Tracker.IsFrozen(ex, at)})
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaseTimer != nil {
		c.leaseTimer.Stop()
		c.leaseTimer = nil
	}
	if next, ok := c.Tracker.NextChangeAt(at); ok && !c.closed {
		d := time.Duration(math.Max(1, math.Ceil(next-at))) * time.Millisecond
		c.leaseTimer = time.AfterFunc(d, func() { c.post(c.levelsChanged) })
	}
}

func (c *Client) onClose(code int) {
	c.post(func() { c.emit("close", code) })
	c.mu.Lock()
	stop := c.closed || !c.o.Reconnect
	c.mu.Unlock()
	if stop {
		return
	}
	go func() {
		wait := time.Second
		if n := c.Tracker.Notice(); n != nil && time.Duration(n.InMs)*time.Millisecond > wait {
			wait = time.Duration(n.InMs) * time.Millisecond
		}
		for {
			select {
			case <-time.After(wait):
			case <-c.done:
				return
			}
			if err := c.open(); err == nil {
				return
			} else {
				c.postErr(err)
			}
			if wait *= 2; wait > 30*time.Second {
				wait = 30 * time.Second
			}
		}
	}()
}

// Request is the low-level call on the current transport. Returns the reply body or an *ApiError.
func (c *Client) Request(op string, args map[string]any) (map[string]any, error) {
	c.mu.Lock()
	t := c.t
	c.mu.Unlock()
	if t == nil {
		return nil, newErr(0, "not-connected", nil)
	}
	known := false
	for _, x := range Ops {
		if x == op {
			known = true
		}
	}
	if !known {
		return nil, newErr(404, "unknown-op", nil)
	}
	if args == nil {
		args = map[string]any{}
	}
	return t.request(op, args)
}

func (c *Client) Status() (map[string]any, error) { return c.Request("status", nil) }
func (c *Client) Usage() (map[string]any, error)  { return c.Request("usage", nil) }
func (c *Client) Peers() (map[string]any, error)  { return c.Request("peers", nil) }
func (c *Client) CondsList() ([]any, error) {
	r, err := c.Request("conds.list", nil)
	if err != nil {
		return nil, err
	}
	items, _ := r["items"].([]any)
	return items, nil
}
func (c *Client) CondsAdd(cond map[string]any, ex string) (map[string]any, error) {
	return c.Request("conds.add", map[string]any{"cond": cond, "ex": ex})
}
func (c *Client) CondsDel(slot string) (map[string]any, error) {
	return c.Request("conds.del", map[string]any{"slot": slot})
}
func (c *Client) SignalsSince(since int64, limit int) (map[string]any, error) {
	return c.Request("signals.since", map[string]any{"since": since, "limit": limit})
}

// ReportUsage reports the exchange weight spent since your last report (shown to the agent as "external"). Needs Name.
func (c *Client) ReportUsage(ex string, w float64, bucket string) (map[string]any, error) {
	id := c.ClientID()
	if id == "" {
		return nil, newErr(0, "not-registered", map[string]any{"detail": "connect with a name to report usage"})
	}
	if bucket == "" {
		bucket = "rest"
	}
	return c.Request("usage.report", map[string]any{"clientId": id, "ex": ex, "bucket": bucket, "w": w})
}

// Level is "free" | "soft" | "hard" | "unknown".
func (c *Client) Level(ex string) string { return c.Tracker.Level(ex, c.now()) }

// Frozen reports whether the agent is backing off this exchange.
func (c *Client) Frozen(ex string) bool { return c.Tracker.IsFrozen(ex, c.now()) }

// Why lists the causes in effect.
func (c *Client) Why(ex string) []string { return c.Tracker.Why(ex, c.now()) }

// Clear blocks until ex is below `below` ("hard" when empty) or the context ends. "unknown" never blocks.
func (c *Client) Clear(ctx context.Context, ex, below string) bool {
	if below == "" {
		below = "hard"
	}
	ok := func() bool { l := c.Level(ex); return l == "unknown" || rank[l] < rank[below] }
	ch := make(chan struct{}, 1)
	k := norm(ex)
	off := c.On("level", func(v any) {
		if e, _ := v.(LevelEvent); e.Ex == k && ok() {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})
	defer off()
	if ok() {
		return true
	}
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return ok()
	}
}

// OnLevel calls fn on every change for ex. Returns unsubscribe.
func (c *Client) OnLevel(ex string, fn func(LevelEvent)) func() {
	k := norm(ex)
	return c.On("level", func(v any) {
		if e, ok := v.(LevelEvent); ok && e.Ex == k {
			fn(e)
		}
	})
}

// Close stops the client.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.leaseTimer != nil {
		c.leaseTimer.Stop()
	}
	if c.regTimer != nil {
		c.regTimer.Stop()
	}
	t := c.t
	c.mu.Unlock()
	if t != nil {
		t.close()
	}
	close(c.done)
}

func errOf(status int, body []byte) *ApiError {
	var j map[string]any
	_ = json.Unmarshal(body, &j)
	code := "http-" + strconv.Itoa(status)
	if j != nil && j["error"] != nil {
		code = toString(j["error"])
	}
	return newErr(status, code, j)
}

// ── REST transport — request/response over HTTP, events over SSE (/v1/events) ──

type restT struct {
	c      *Client
	base   string
	auth   string
	hc     *http.Client
	mu     sync.Mutex
	cancel context.CancelFunc
	closed bool
}

func newREST(c *Client, e Endpoint) *restT {
	return &restT{c: c, base: strings.TrimRight(e.URL, "/"), auth: "Bearer " + e.Token, hc: &http.Client{}}
}

func (r *restT) kind() string         { return "rest" }
func (r *restT) hello() map[string]any { return nil }

func (r *restT) request(op string, a map[string]any) (map[string]any, error) {
	var method, path string
	var body any
	switch op {
	case "status":
		method, path = "GET", "/v1/status"
	case "conds.list":
		method, path = "GET", "/v1/conds"
	case "conds.add":
		method, path, body = "POST", "/v1/conds", map[string]any{"cond": a["cond"], "ex": a["ex"]}
	case "conds.del":
		method, path = "DELETE", "/v1/conds/"+url.PathEscape(toString(a["slot"]))
	case "signals.since":
		since, limit := a["since"], a["limit"]
		if since == nil {
			since = 0
		}
		if limit == nil {
			limit = 100
		}
		method, path = "GET", "/v1/signals?since="+url.QueryEscape(fmt.Sprint(since))+"&limit="+url.QueryEscape(fmt.Sprint(limit))
	case "peers":
		method, path = "GET", "/v1/peers"
	case "usage":
		method, path = "GET", "/v1/usage"
	case "control.state":
		method, path = "GET", "/v1/control"
	case "control.ack":
		method, path, body = "POST", "/v1/control/ack", a
	case "client.register":
		method, path, body = "POST", "/v1/clients", a
	case "usage.report":
		method, path, body = "POST", "/v1/usage/report", a
	default:
		return nil, newErr(404, "unknown-op", nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.c.o.Timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.base+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", r.auth)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := r.hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, newErr(0, "client-timeout", nil)
		}
		return nil, newErr(0, "unreachable", map[string]any{"detail": err.Error()})
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, newErr(0, "unreachable", map[string]any{"detail": err.Error()})
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, errOf(res.StatusCode, data)
	}
	var j map[string]any
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, newErr(res.StatusCode, "bad-json", nil)
	}
	return j, nil
}

func (r *restT) stream(since int64) error {
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, "GET", r.base+"/v1/events", nil)
	if err != nil {
		cancel()
		return err
	}
	req.Header.Set("Authorization", r.auth)
	req.Header.Set("Accept", "text/event-stream")
	if since > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(since, 10))
	}
	res, err := r.hc.Do(req)
	if err != nil {
		cancel()
		return newErr(0, "unreachable", map[string]any{"detail": err.Error()})
	}
	if res.StatusCode != 200 {
		data, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		cancel()
		return errOf(res.StatusCode, data)
	}
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()
	p := NewSseParser(func(e SseEvent) {
		var d map[string]any
		if json.Unmarshal([]byte(e.Data), &d) != nil {
			return
		}
		r.c.post(func() { r.c.onEvent(e.Event, d) })
	})
	go func() {
		defer res.Body.Close()
		buf := make([]byte, 8192)
		var pend []byte
		for {
			n, err := res.Body.Read(buf)
			if n > 0 {
				pend = append(pend, buf[:n]...)
				// feed only complete UTF-8 sequences — a chunk can end inside one
				cut := len(pend)
				for i := 1; i <= 3 && i <= len(pend); i++ {
					b := pend[len(pend)-i]
					if b&0xC0 == 0x80 {
						continue
					}
					if b >= 0xC0 && !utf8.FullRune(pend[len(pend)-i:]) {
						cut = len(pend) - i
					}
					break
				}
				p.Feed(string(pend[:cut]))
				pend = append([]byte(nil), pend[cut:]...)
			}
			if err != nil {
				break
			}
		}
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if !closed {
			r.c.onClose(1006)
		}
	}()
	return nil
}

func (r *restT) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.cancel != nil {
		r.cancel()
	}
}

// ── WebSocket transport (RFC 6455, client side) — the daemon's /v1 endpoint ──

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsT struct {
	c      *Client
	conn   net.Conn
	br     *bufio.Reader
	wmu    sync.Mutex
	mu     sync.Mutex
	wait   map[int64]chan wsReply
	id     int64
	closed bool
	h      map[string]any
}

type wsReply struct {
	body map[string]any
	err  error
}

func dialWS(c *Client, e Endpoint) (*wsT, error) {
	u, err := url.Parse(e.URL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") {
		return nil, newErr(0, "unreachable", map[string]any{"detail": "ws url must start with ws:// or wss://"})
	}
	host := u.Host
	if u.Port() == "" {
		if u.Scheme == "wss" {
			host = net.JoinHostPort(u.Hostname(), "443")
		} else {
			host = net.JoinHostPort(u.Hostname(), "80")
		}
	}
	d := net.Dialer{Timeout: c.o.Timeout}
	var conn net.Conn
	if u.Scheme == "wss" {
		conn, err = tls.DialWithDialer(&d, "tcp", host, &tls.Config{ServerName: u.Hostname()})
	} else {
		conn, err = d.Dial("tcp", host)
	}
	if err != nil {
		return nil, newErr(0, "unreachable", map[string]any{"detail": err.Error()})
	}
	kb := make([]byte, 16)
	_, _ = rand.Read(kb)
	key := base64.StdEncoding.EncodeToString(kb)
	path := u.RequestURI()
	if path == "" || path == "/" {
		path = "/v1"
	}
	// No Origin header: the daemon refuses requests that carry one.
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, u.Host, key)
	_ = conn.SetReadDeadline(time.Now().Add(c.o.Timeout))
	br := bufio.NewReader(conn)
	res, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		conn.Close()
		return nil, newErr(0, "unreachable", map[string]any{"detail": err.Error()})
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	if res.StatusCode != 101 || res.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]) {
		conn.Close()
		return nil, newErr(0, "unreachable", map[string]any{"detail": "upgrade refused (" + res.Status + ")"})
	}
	_ = conn.SetReadDeadline(time.Time{})
	w := &wsT{c: c, conn: conn, br: br, wait: map[int64]chan wsReply{}}
	go w.readLoop()
	h, err := w.send(map[string]any{"op": "auth", "token": e.Token})
	if err != nil {
		w.close()
		return nil, err
	}
	w.h = h
	return w, nil
}

func (w *wsT) kind() string         { return "ws" }
func (w *wsT) hello() map[string]any { return w.h }
func (w *wsT) stream(int64) error    { return nil }

func (w *wsT) writeFrame(op byte, payload []byte) error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	var hdr []byte
	n := len(payload)
	switch {
	case n < 126:
		hdr = []byte{0x80 | op, 0x80 | byte(n)}
	case n < 65536:
		hdr = []byte{0x80 | op, 0x80 | 126, byte(n >> 8), byte(n)}
	default:
		hdr = make([]byte, 10)
		hdr[0], hdr[1] = 0x80|op, 0x80|127
		binary.BigEndian.PutUint64(hdr[2:], uint64(n))
	}
	mask := make([]byte, 4)
	_, _ = rand.Read(mask)
	out := make([]byte, 0, len(hdr)+4+n)
	out = append(append(out, hdr...), mask...)
	for i, b := range payload {
		out = append(out, b^mask[i&3])
	}
	_, err := w.conn.Write(out)
	return err
}

func (w *wsT) readFrame() (fin bool, op byte, data []byte, err error) {
	h := make([]byte, 2)
	if _, err = io.ReadFull(w.br, h); err != nil {
		return
	}
	if h[1]&0x80 != 0 {
		err = errors.New("server frame is masked")
		return
	}
	n := uint64(h[1] & 0x7f)
	switch n {
	case 126:
		b := make([]byte, 2)
		if _, err = io.ReadFull(w.br, b); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(b))
	case 127:
		b := make([]byte, 8)
		if _, err = io.ReadFull(w.br, b); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(b)
	}
	if n > 1<<20 {
		err = errors.New("frame too big")
		return
	}
	data = make([]byte, n)
	_, err = io.ReadFull(w.br, data)
	return h[0]&0x80 != 0, h[0] & 0x0f, data, err
}

func (w *wsT) readLoop() {
	code := 1006
	var parts []byte
	for {
		fin, op, data, err := w.readFrame()
		if err != nil {
			break
		}
		if op == 8 {
			code = 1005
			if len(data) >= 2 {
				code = int(binary.BigEndian.Uint16(data[:2]))
			}
			break
		}
		if op == 9 {
			_ = w.writeFrame(10, data)
			continue
		}
		if op == 0 || op == 1 {
			parts = append(parts, data...)
			if fin {
				w.onText(parts)
				parts = nil
			}
		}
	}
	w.finish(code)
}

func (w *wsT) onText(b []byte) {
	var m map[string]any
	if json.Unmarshal(b, &m) != nil || m == nil {
		return
	}
	if ev, ok := m["ev"].(string); ok && ev != "" {
		d := m
		if ev == "signal" || ev == "control" {
			d, _ = m["item"].(map[string]any)
		}
		w.c.post(func() { w.c.onEvent(ev, d) })
		return
	}
	idf, ok := m["id"].(float64)
	if !ok {
		return
	}
	w.mu.Lock()
	ch := w.wait[int64(idf)]
	delete(w.wait, int64(idf))
	w.mu.Unlock()
	if ch == nil {
		return
	}
	body, _ := m["body"].(map[string]any)
	if okv, _ := m["ok"].(bool); okv {
		if body == nil {
			body = map[string]any{}
		}
		ch <- wsReply{body: body}
		return
	}
	code := "error"
	if body != nil && body["error"] != nil {
		code = toString(body["error"])
	} else if m["error"] != nil {
		code = toString(m["error"])
	}
	st, _ := m["status"].(float64)
	ch <- wsReply{err: newErr(int(st), code, body)}
}

func (w *wsT) finish(code int) {
	w.mu.Lock()
	was := w.closed
	w.closed = true
	for id, ch := range w.wait {
		ch <- wsReply{err: newErr(0, "closed", map[string]any{"code": code})}
		delete(w.wait, id)
	}
	w.mu.Unlock()
	w.conn.Close()
	if !was {
		w.c.onClose(code)
	}
}

func (w *wsT) send(msg map[string]any) (map[string]any, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, newErr(0, "closed", nil)
	}
	w.id++
	id := w.id
	ch := make(chan wsReply, 1)
	w.wait[id] = ch
	w.mu.Unlock()
	msg["id"] = id
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if err := w.writeFrame(1, b); err != nil {
		return nil, newErr(0, "closed", map[string]any{"detail": err.Error()})
	}
	select {
	case r := <-ch:
		return r.body, r.err
	case <-time.After(w.c.o.Timeout):
		w.mu.Lock()
		delete(w.wait, id)
		w.mu.Unlock()
		return nil, newErr(0, "client-timeout", nil)
	}
}

func (w *wsT) request(op string, args map[string]any) (map[string]any, error) {
	return w.send(map[string]any{"op": op, "args": args})
}

func (w *wsT) close() {
	w.mu.Lock()
	was := w.closed
	w.closed = true
	w.mu.Unlock()
	if !was {
		_ = w.writeFrame(8, []byte{0x03, 0xe8})
	}
	w.conn.Close()
}
