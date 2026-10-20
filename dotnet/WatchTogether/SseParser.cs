using System;
using System.Collections.Generic;
using System.Text;

namespace WatchTogether;

/// <summary>One Server-Sent Event. <c>Id</c> is null when the block had no id field.</summary>
public sealed record SseEvent(string Event, string? Id, string Data);

/// <summary>Incremental Server-Sent Events parser. Vectors: ../../conformance/sse/*.json</summary>
public sealed class SseParser
{
    readonly Action<SseEvent> _on;
    readonly StringBuilder _buf = new();
    List<string> _data = new();
    string? _event, _id;
    bool _has;

    public string? LastEventId { get; private set; }

    public SseParser(Action<SseEvent> on) => _on = on;

    void Line(string l)
    {
        if (l.Length == 0)
        {
            if (_has) _on(new SseEvent(_event ?? "message", _id, string.Join("\n", _data)));
            _data = new(); _event = null; _id = null; _has = false;
            return;
        }
        if (l[0] == ':') return;
        var i = l.IndexOf(':');
        var field = i < 0 ? l : l[..i];
        var value = i < 0 ? "" : l[(i + 1)..];
        if (value.StartsWith(' ')) value = value[1..];
        switch (field)
        {
            case "data": _data.Add(value); _has = true; break;
            case "event": _event = value; break;
            case "id": _id = value; LastEventId = value; break;
        }
    }

    public void Feed(string chunk)
    {
        _buf.Append(chunk);
        while (true)
        {
            int n = -1, len = 0;
            for (var k = 0; k < _buf.Length; k++)
            {
                var c = _buf[k];
                if (c == '\n') { n = k; len = 1; break; }
                if (c == '\r')
                {
                    if (k == _buf.Length - 1) return; // may be the first half of \r\n — wait for more
                    n = k; len = _buf[k + 1] == '\n' ? 2 : 1; break;
                }
            }
            if (n < 0) return;
            var l = _buf.ToString(0, n);
            _buf.Remove(0, n + len);
            Line(l);
        }
    }
}
