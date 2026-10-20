package watchtogether

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

const root = "../conformance"

func vectors(t *testing.T, dir string) map[string]map[string]any {
	files, err := filepath.Glob(filepath.Join(root, dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no vectors in %s", dir)
	}
	out := map[string]map[string]any{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var v map[string]any
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out[filepath.Base(f)] = v
	}
	return out
}

func sortedKeys(m map[string]map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func acksJSON(a []Ack) []any {
	out := []any{}
	for _, x := range a {
		out = append(out, map[string]any{"epoch": float64(x.Epoch), "ex": x.Ex, "level": x.Level})
	}
	return out
}

func noticeJSON(n *Notice) any {
	if n == nil {
		return nil
	}
	var why any
	if n.Why != nil {
		why = *n.Why
	}
	return map[string]any{"kind": n.Kind, "inMs": float64(n.InMs), "why": why}
}

func TestControlVectors(t *testing.T) {
	vs := vectors(t, "control")
	if len(vs) < 7 {
		t.Fatalf("only %d control vectors", len(vs))
	}
	for _, name := range sortedKeys(vs) {
		v := vs[name]
		uses := []string{}
		if u, ok := v["uses"].([]any); ok {
			for _, x := range u {
				uses = append(uses, x.(string))
			}
		}
		tr := NewControlTracker(uses)
		last := -1e18
		for i, raw := range v["steps"].([]any) {
			s := raw.(map[string]any)
			at := s["at"].(float64)
			if at < last {
				t.Errorf("%s step %d: steps out of order", name, i)
			}
			last = at
			if c, ok := s["control"].(map[string]any); ok {
				tr.Apply(c, at)
			}
			if u, _ := s["unsupported"].(bool); u {
				tr.SetUnsupported()
			}
			e, ok := s["expect"].(map[string]any)
			if !ok {
				continue
			}
			tr.Advance(at)
			if k, ok := e["known"]; ok && tr.Known() != k.(bool) {
				t.Errorf("%s step %d known: got %v", name, i, tr.Known())
			}
			if lv, ok := e["level"].(map[string]any); ok {
				for ex, want := range lv {
					if got := tr.Level(ex, at); got != want {
						t.Errorf("%s step %d level %s: got %s want %v", name, i, ex, got, want)
					}
				}
			}
			if fz, ok := e["frozen"].(map[string]any); ok {
				for ex, want := range fz {
					if got := tr.IsFrozen(ex, at); got != want.(bool) {
						t.Errorf("%s step %d frozen %s: got %v", name, i, ex, got)
					}
				}
			}
			if want, ok := e["notice"]; ok {
				if got := noticeJSON(tr.Notice()); !reflect.DeepEqual(got, want) {
					t.Errorf("%s step %d notice: got %v want %v", name, i, got, want)
				}
			}
			if want, ok := e["acks"]; ok {
				if got := acksJSON(tr.DrainAcks()); !reflect.DeepEqual(got, want) {
					t.Errorf("%s step %d acks: got %v want %v", name, i, got, want)
				}
			}
		}
	}
}

func TestSseVectors(t *testing.T) {
	vs := vectors(t, "sse")
	for _, name := range sortedKeys(vs) {
		v := vs[name]
		got := []any{}
		p := NewSseParser(func(e SseEvent) {
			var id any
			if e.ID != nil {
				id = *e.ID
			}
			got = append(got, map[string]any{"event": e.Event, "id": id, "data": e.Data})
		})
		for _, c := range v["chunks"].([]any) {
			p.Feed(c.(string))
		}
		if !reflect.DeepEqual(got, v["events"]) {
			t.Errorf("%s: got %v want %v", name, got, v["events"])
		}
	}
}

func TestOps(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(root, "ops.json"))
	if err != nil {
		t.Fatal(err)
	}
	var j struct {
		Ops []string `json:"ops"`
	}
	if err := json.Unmarshal(b, &j); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(j.Ops, Ops) {
		t.Errorf("ops: got %v want %v", Ops, j.Ops)
	}
}
