"""Server-Sent Events parser - pure, incremental. Vectors: ../../conformance/sse/*.json

`id` on an emitted event is the id field of that block (None when absent); `last_event_id` keeps the last one seen.
"""

import re

_EOL = re.compile(r"\r\n|\r|\n")


class SseParser:
    def __init__(self, on_event):
        self._on = on_event
        self._buf = ""
        self._data = []
        self._event = None
        self._id = None
        self._has = False
        self.last_event_id = None

    def _line(self, ln):
        if ln == "":
            if self._has:
                self._on({"event": self._event or "message", "id": self._id, "data": "\n".join(self._data)})
            self._data, self._event, self._id, self._has = [], None, None, False
            return
        if ln[0] == ":":
            return
        field, sep, value = ln.partition(":")
        if sep and value[:1] == " ":
            value = value[1:]
        if field == "data":
            self._data.append(value)
            self._has = True
        elif field == "event":
            self._event = value
        elif field == "id":
            self._id = value
            self.last_event_id = value

    def feed(self, chunk):
        self._buf += chunk
        while True:
            m = _EOL.search(self._buf)
            if not m:
                break
            # a lone \r at the very end may be the first half of \r\n - wait for more
            if m.group(0) == "\r" and m.start() == len(self._buf) - 1:
                break
            self._line(self._buf[:m.start()])
            self._buf = self._buf[m.end():]
