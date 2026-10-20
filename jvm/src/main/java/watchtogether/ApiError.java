package watchtogether;

import java.util.Map;

/** An error reply. {@code code} is machine-readable ({@code body.error}); read codes, not message text. */
public final class ApiError extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public final int status;
    public final String code;
    public final String reason;
    public final transient Map<String, Object> body;

    public ApiError(int status, String code, Map<String, Object> body) {
        super(code + (body != null && body.get("reason") != null ? " (" + body.get("reason") + ")" : "") + " [" + status + "]");
        this.status = status;
        this.code = code;
        this.body = body;
        this.reason = body != null && body.get("reason") != null ? String.valueOf(body.get("reason")) : null;
    }
}
