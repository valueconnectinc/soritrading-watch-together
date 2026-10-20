import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createControlTracker } from '../src/control.mjs';
import { createSseParser } from '../src/sse.mjs';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'conformance');
const files = (d) => fs.readdirSync(path.join(root, d)).filter((f) => f.endsWith('.json')).sort().map((f) => [f, JSON.parse(fs.readFileSync(path.join(root, d, f), 'utf8'))]);

for (const [f, v] of files('control')) {
  test('control/' + f + ' — ' + v.name, () => {
    const t = createControlTracker({ uses: v.uses || [] });
    let last = -Infinity;
    for (const [i, s] of v.steps.entries()) {
      assert.ok(s.at >= last, 'steps must be in time order (step ' + i + ')'); last = s.at;
      if (s.control) t.apply(s.control, s.at);
      if (s.unsupported) t.setUnsupported();
      if (s.expect) {
        t.advance(s.at);
        const e = s.expect, at = s.at, where = f + ' step ' + i;
        if ('known' in e) assert.equal(t.known, e.known, where + ' known');
        for (const [ex, l] of Object.entries(e.level || {})) assert.equal(t.level(ex, at), l, where + ' level ' + ex);
        for (const [ex, z] of Object.entries(e.frozen || {})) assert.equal(t.isFrozen(ex, at), z, where + ' frozen ' + ex);
        if ('notice' in e) assert.deepEqual(t.notice, e.notice, where + ' notice');
        if ('acks' in e) assert.deepEqual(t.drainAcks(), e.acks, where + ' acks');
      }
    }
  });
}

for (const [f, v] of files('sse')) {
  test('sse/' + f + ' — ' + v.name, () => {
    const got = [];
    const p = createSseParser((e) => got.push(e));
    for (const c of v.chunks) p.feed(c);
    assert.deepEqual(got, v.events);
  });
}
