"""Runs the language-neutral vectors in ../../conformance against the Python tracker and SSE parser."""

import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from watch_together import OPS, ControlTracker, SseParser  # noqa: E402

ROOT = os.path.join(HERE, "..", "..", "conformance")


def vectors(d):
    p = os.path.join(ROOT, d)
    for f in sorted(os.listdir(p)):
        if f.endswith(".json"):
            with open(os.path.join(p, f), encoding="utf-8") as fh:
                yield f, json.load(fh)


class Conformance(unittest.TestCase):
    def test_control(self):
        n = 0
        for f, v in vectors("control"):
            n += 1
            with self.subTest(vector=f):
                t = ControlTracker(v.get("uses") or [])
                last = float("-inf")
                for i, s in enumerate(v["steps"]):
                    self.assertGreaterEqual(s["at"], last, "steps must be in time order (%s step %d)" % (f, i))
                    last = s["at"]
                    at = s["at"]
                    if "control" in s:
                        t.apply(s["control"], at)
                    if s.get("unsupported"):
                        t.set_unsupported()
                    e = s.get("expect")
                    if not e:
                        continue
                    t.advance(at)
                    where = "%s step %d" % (f, i)
                    if "known" in e:
                        self.assertEqual(t.known, e["known"], where + " known")
                    for ex, lv in (e.get("level") or {}).items():
                        self.assertEqual(t.level(ex, at), lv, where + " level " + ex)
                    for ex, z in (e.get("frozen") or {}).items():
                        self.assertEqual(t.is_frozen(ex, at), z, where + " frozen " + ex)
                    if "notice" in e:
                        self.assertEqual(t.notice, e["notice"], where + " notice")
                    if "acks" in e:
                        self.assertEqual(t.drain_acks(), e["acks"], where + " acks")
        self.assertGreaterEqual(n, 7, "vectors found")

    def test_sse(self):
        n = 0
        for f, v in vectors("sse"):
            n += 1
            with self.subTest(vector=f):
                got = []
                p = SseParser(got.append)
                for c in v["chunks"]:
                    p.feed(c)
                self.assertEqual(got, v["events"])
        self.assertGreaterEqual(n, 1)

    def test_ops(self):
        with open(os.path.join(ROOT, "ops.json"), encoding="utf-8") as fh:
            self.assertEqual(list(OPS), json.load(fh)["ops"])


if __name__ == "__main__":
    unittest.main()
