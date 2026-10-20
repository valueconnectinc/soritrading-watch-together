package watchtogether

import "strings"

// SseEvent is one Server-Sent Event. ID is nil when the block had no id field.
type SseEvent struct {
	Event string
	ID    *string
	Data  string
}

// SseParser is an incremental Server-Sent Events parser. Vectors: ../conformance/sse/*.json
type SseParser struct {
	on          func(SseEvent)
	buf         string
	data        []string
	event       string
	id          *string
	has         bool
	LastEventID *string
}

// NewSseParser calls on for every complete event.
func NewSseParser(on func(SseEvent)) *SseParser { return &SseParser{on: on} }

func (p *SseParser) line(l string) {
	if l == "" {
		if p.has {
			ev := p.event
			if ev == "" {
				ev = "message"
			}
			p.on(SseEvent{Event: ev, ID: p.id, Data: strings.Join(p.data, "\n")})
		}
		p.data, p.event, p.id, p.has = nil, "", nil, false
		return
	}
	if l[0] == ':' {
		return
	}
	field, value := l, ""
	if i := strings.IndexByte(l, ':'); i >= 0 {
		field, value = l[:i], l[i+1:]
		value = strings.TrimPrefix(value, " ")
	}
	switch field {
	case "data":
		p.data = append(p.data, value)
		p.has = true
	case "event":
		p.event = value
	case "id":
		v := value
		p.id = &v
		p.LastEventID = &v
	}
}

// Feed adds a chunk of the stream.
func (p *SseParser) Feed(chunk string) {
	p.buf += chunk
	for {
		i := strings.IndexAny(p.buf, "\r\n")
		if i < 0 {
			return
		}
		n := 1
		if p.buf[i] == '\r' {
			if i == len(p.buf)-1 {
				return // may be the first half of \r\n — wait for more
			}
			if p.buf[i+1] == '\n' {
				n = 2
			}
		}
		l := p.buf[:i]
		p.buf = p.buf[i+n:]
		p.line(l)
	}
}
