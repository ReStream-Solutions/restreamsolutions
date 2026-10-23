from typing import Optional


class RestreamError(Exception):
    """Base class for all exceptions raised by the SDK."""


class AuthError(RestreamError):
    """Raised when authentication via token fails (HTTP 401, or HTTP 403 on a WebSocket handshake)."""

    def __init__(
        self,
        message: str = "Unauthorized: invalid or missing authentication token",
        rejected_token: Optional[str] = None,
    ):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
            rejected_token: The access token rejected by the server, if known. It lets the SDK skip the token
                request when another call has already replaced this token. It is never included in the message.
        """
        super().__init__(message)
        self.rejected_token = rejected_token


class ForbiddenError(AuthError):
    """Raised when the token is valid but has no access to the requested resource (HTTP 403).

    Subclasses AuthError for backward compatibility, but it is never retried because
    requesting a new token does not change the permissions of the client.
    """

    def __init__(self, message: str = "Forbidden: the client has no access to the requested resource"):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
        """
        super().__init__(message)


class CredentialsError(RestreamError):
    """Raised when OAuth2 credentials are not provided or rejected by the server."""

    def __init__(self, message: str = "Unauthorized: invalid or missing client_id and/or client_secret"):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
        """
        super().__init__(message)


class APICompatibilityError(RestreamError):
    """Raised when the SDK's expectations don't match the server API (schema/version mismatch)."""

    def __init__(
        self, message: str = "API compatibility error: server response does not match expected schema or version"
    ):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
        """
        super().__init__(message)


class NotFoundError(APICompatibilityError):
    """Raised when the server returns HTTP 404: the endpoint or the requested object does not exist."""

    def __init__(self, message: str = "Not found: the endpoint or the requested object does not exist"):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
        """
        super().__init__(message)


class BadRequestError(RestreamError):
    """Raised for HTTP 4xx responses that are not mapped to a more specific exception, e.g. 400 Bad Request.

    Such responses mean the request itself is wrong, so they are never retried.
    """

    def __init__(self, message: str = "Bad request", status_code: Optional[int] = None):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
            status_code: HTTP status code returned by the server, if known.
        """
        super().__init__(message)
        self.status_code = status_code


class APIConcurrencyLimitError(RestreamError):
    """Raised when the server rejects the request because of the per-client concurrency limit (HTTP 429)."""

    def __init__(
        self,
        message: str = "Too many concurrent requests. Please try again later.",
        retry_after: Optional[float] = None,
    ):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
            retry_after: Delay suggested by the server via the Retry-After header, in seconds, if any.
        """
        super().__init__(message)
        self.retry_after = retry_after


class WebsocketError(RestreamError):
    """Raised when a websocket error occurs."""

    def __init__(self, message: str = "The websocket error occurs."):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
        """
        super().__init__(message)


class ServerError(RestreamError):
    """Raised when the server returns HTTP 5xx."""

    def __init__(self, message: str = "Server error occurred.", status_code: Optional[int] = None):
        """Initialize the exception.

        Args:
            message: Human-readable description of the error.
            status_code: HTTP status code returned by the server, if known.
        """
        super().__init__(message)
        self.status_code = status_code
