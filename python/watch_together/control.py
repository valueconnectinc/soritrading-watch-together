"""Control-plane tracker - pure. Turns `control` states into a per-exchange level on the caller's monotonic clock.

Spec: ../../docs/control-plane.md - vectors: ../../conformance/control/*.json
"""

LEVELS = ("free", "soft", "hard")
_RANK = {"free": 0, "soft": 1, "hard": 2}
ANY = "*"


def _norm(ex):
    s = ("" if ex is None else str(ex)).strip().lower()
    return s or ANY


def _num(v, default=0.0):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return f if f == f and f not in (float("inf"), float("-inf")) else default


class ControlTracker:
    """`uses`: exchanges this program calls itself - only these are acknowledged."""

    def __init__(self, uses=()):
        self._known = False
        self._unsupported = False
        self._boot = None
        self._epoch = -1
        self._table = {}
        self._frozen = {}
        self._notice = None
        self._prev = {_norm(ex): "free" for ex in uses}
        self._acks = []

    # -- reading --------------------------------------------------------
    def level(self, ex, at):
        """'free' | 'soft' | 'hard' | 'unknown' at monotonic time `at` (ms)."""
        if not self._known:
            return "unknown"
        best = "free"
        for k in (_norm(ex), ANY):
            e = self._table.get(k)
            if e and at < e["until"] and _RANK[e["level"]] > _RANK[best]:
                best = e["level"]
        return best

    def is_frozen(self, ex, at):
        f = self._frozen.get(_norm(ex))
        return bool(f and at < f["until"])

    def why(self, ex, at):
        out = []
        for k in (_norm(ex), ANY):
            e = self._table.get(k)
            if e and at < e["until"]:
                for w in e["why"]:
                    if w not in out:
                        out.append(w)
        return out

    def next_change_at(self, at):
        """Earliest time after `at` when a level or frozen flag can change without a new message (for a timer)."""
        ts = [e["until"] for e in self._table.values() if e["until"] > at]
        ts += [f["until"] for f in self._frozen.values() if f["until"] > at]
        return min(ts) if ts else None

    @property
    def known(self):
        return self._known

    @property
    def unsupported(self):
        return self._unsupported

    @property
    def notice(self):
        return self._notice

    @property
    def epoch(self):
        return self._epoch if self._known else None

    @property
    def boot(self):
        return self._boot

    # -- writing --------------------------------------------------------
    def advance(self, at):
        if not self._known:
            return
        for ex, p in list(self._prev.items()):
            lv = self.level(ex, at)
            if lv != p:
                self._prev[ex] = lv
                self._acks.append({"epoch": self._epoch, "ex": ex, "level": lv})

    def apply(self, c, at):
        """Apply a full control state received at `at`. Returns False when ignored (older epoch in the same boot)."""
        if not isinstance(c, dict):
            return False
        b = None if c.get("boot") is None else str(c.get("boot"))
        try:
            e = float(c.get("epoch"))
        except (TypeError, ValueError):
            return False
        if e != e or e in (float("inf"), float("-inf")):
            return False
        e = int(e) if e == int(e) else e
        if b == self._boot and self._known and e < self._epoch:
            return False
        self._boot, self._epoch, self._known, self._unsupported = b, e, True, False
        self._table = {}
        for ex, y in (c.get("yield") or {}).items():
            if not isinstance(y, dict) or y.get("level") not in ("soft", "hard"):
                continue
            why = list(y["why"]) if isinstance(y.get("why"), list) else []
            self._table[_norm(ex)] = {"level": y["level"], "until": at + max(0.0, _num(y.get("ttlMs"))), "why": why}
        self._frozen = {}
        for ex, f in (c.get("frozen") or {}).items():
            if isinstance(f, dict) and _num(f.get("untilMs")) > 0:
                self._frozen[_norm(ex)] = {"until": at + _num(f.get("untilMs")),
                                          "why": None if f.get("why") is None else str(f.get("why"))}
        n = c.get("notice")
        if isinstance(n, dict) and n.get("kind"):
            in_ms = _num(n.get("inMs"))
            self._notice = {"kind": str(n["kind"]), "inMs": int(in_ms) if in_ms == int(in_ms) else in_ms,
                            "why": None if n.get("why") is None else str(n.get("why"))}
        else:
            self._notice = None
        self.advance(at)
        return True

    def set_unsupported(self):
        """The agent has no control plane (control.state answered 501/404). Levels stay `unknown` - never `free`."""
        self._unsupported = True
        self._known = False

    def drain_acks(self):
        a, self._acks = self._acks, []
        return a
