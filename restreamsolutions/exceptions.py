class AuthError(Exception):
    """Raised when authentication via token fails (e.g., HTTP 401)."""

    def __init__(self, message: str = "Unauthorized: invalid or missing authentication token"):
        super().__init__(message)


class CredentialsError(Exception):
    """Raised when Oauth2 credentials are not provided."""

    def __init__(self, message: str = "Unauthorized: invalid or missing client_id and/or client_secret"):
        super().__init__(message)


class APICompatibilityError(Exception):
    """Raised when the SDK's expectations don't match the server API (schema/version mismatch)."""

    def __init__(
        self, message: str = "API compatibility error: server response does not match expected schema or version"
    ):
        super().__init__(message)


class APIConcurrencyLimitError(Exception):
    """Raised when the SDK's sends to many concurrent requests."""

    def __init__(self, message: str = "Too many concurrent requests. Please try again later."):
        super().__init__(message)


class WebsocketError(Exception):
    """Raised when a websocket error occurs."""

    def __init__(self, message: str = "The websocket error occurs."):
        super().__init__(message)


class ServerError(Exception):
    """Raised when a server error occurs."""

    def __init__(self, message: str = "Server error occurred."):
        super().__init__(message)
