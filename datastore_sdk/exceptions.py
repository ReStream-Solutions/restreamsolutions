class AuthError(Exception):
    """Raised when authentication via token fails (e.g., HTTP 401)."""

    def __init__(self, message: str = "Unauthorized: invalid or missing authentication token"):
        super().__init__(message)


class APICompatibilityError(Exception):
    """Raised when the SDK's expectations don't match the server API (schema/version mismatch)."""

    def __init__(self, message: str = "API compatibility error: server response does not match expected schema or version"):
        super().__init__(message)
