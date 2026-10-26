"""Retry policy shared by HTTP requests, streaming responses and WebSocket connections."""

import asyncio
import functools
import inspect
import logging
import os
import random
import time
from typing import Any, Awaitable, Callable, Optional

import aiohttp
import requests
from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, NewConnectionError

from .constants import (
    AUTH_MAX_RETRY_ATTEMPTS,
    MAX_RETRY_ATTEMPTS,
    MAX_RETRY_ATTEMPTS_ENV_VAR,
    RETRY_AFTER_MAX_SECONDS,
    RETRY_BACKOFF_FACTOR,
    RETRY_INITIAL_DELAY_SECONDS,
    RETRY_JITTER,
    RETRY_MAX_DELAY_SECONDS,
)
from .exceptions import (
    APICompatibilityError,
    APIConcurrencyLimitError,
    AuthError,
    BadRequestError,
    CredentialsError,
    ForbiddenError,
)

logger = logging.getLogger(__name__)

# Exceptions that mean the request itself is wrong or the SDK is misused; repeating them is pointless.
# Any other exception is treated as transient, so new failure modes of the network libraries need no special handling.
_FATAL_EXCEPTIONS = (
    CredentialsError,
    APICompatibilityError,
    ForbiddenError,
    BadRequestError,
    ValueError,
    TypeError,
)


def resolve_max_attempts(default: int = MAX_RETRY_ATTEMPTS) -> int:
    """Return the maximum number of attempts, honoring the MAX_RETRY_ATTEMPTS_ENV_VAR environment variable.

    Args:
        default: Value to use when the variable is not set or holds an invalid value.

    Returns:
        A positive integer number of attempts.
    """
    raw_value = os.environ.get(MAX_RETRY_ATTEMPTS_ENV_VAR)
    if raw_value is None:
        return default
    try:
        attempts = int(raw_value)
        if attempts < 1:
            raise ValueError
    except ValueError:
        logger.warning(
            '%s=%r is not a positive integer, falling back to %s attempts',
            MAX_RETRY_ATTEMPTS_ENV_VAR,
            raw_value,
            default,
        )
        return default
    return attempts


def _is_connect_phase_error(exc: BaseException) -> bool:
    """Return True if the exception was raised before the request reached the server.

    Such errors are safe to retry even for non-idempotent requests because the server
    has not processed anything.

    Args:
        exc: The exception to classify.

    Returns:
        True for connection establishment errors, False otherwise.
    """
    if isinstance(exc, (aiohttp.ClientConnectorError, requests.exceptions.ConnectTimeout)):
        return True
    if isinstance(exc, requests.exceptions.ConnectionError) and exc.args:
        cause = exc.args[0]
        if isinstance(cause, MaxRetryError):
            return isinstance(cause.reason, (NewConnectionError, ConnectTimeoutError))
    return False


class RetryPolicy:
    """Decides whether a failed call should be repeated and how long to wait before repeating it.

    Policy summary:
    - Fatal errors are never retried: credential and API compatibility errors, HTTP 400, 403 and 404,
      ValueError and TypeError.
    - HTTP 401 (and HTTP 403 on a WebSocket handshake) is retried once after refreshing the access token,
      and only if the token is managed by the SDK (no explicit auth_token was passed to the call). The refresh
      corrects the request rather than repeating it, so it does not consume an attempt.
    - HTTP 429 is retried after at least the delay suggested by the Retry-After header, capped by max_retry_after.
    - Any other error (network failures, timeouts, HTTP 5xx, etc.) is retried with exponential backoff and jitter;
      a single delay never exceeds max_delay.
    - For non-idempotent calls only errors raised before the request reached the server are retried,
      plus HTTP 401 and 429 which the server returns before processing the request.
    """

    def __init__(
        self,
        attempts: Optional[int] = None,
        *,
        initial_delay: float = RETRY_INITIAL_DELAY_SECONDS,
        factor: float = RETRY_BACKOFF_FACTOR,
        max_delay: float = RETRY_MAX_DELAY_SECONDS,
        max_retry_after: float = RETRY_AFTER_MAX_SECONDS,
        jitter: bool = RETRY_JITTER,
        idempotent: bool = True,
        refresh_token: Optional[Callable[[], Any]] = None,
        arefresh_token: Optional[Callable[[], Awaitable[Any]]] = None,
    ) -> None:
        """Initialize the policy.

        Args:
            attempts: Total number of attempts including the first call. None means MAX_RETRY_ATTEMPTS,
                which can be overridden by the MAX_RETRY_ATTEMPTS_ENV_VAR environment variable. When a
                value is given explicitly, it acts as an upper bound for the environment override.
            initial_delay: Delay before the first retry, in seconds.
            factor: Multiplier applied to the delay after each failed attempt; must be >= 1.
            max_delay: Upper bound for a single delay, in seconds, including the jitter.
            max_retry_after: Upper bound for the delay taken from the Retry-After header, in seconds.
            jitter: If True, each delay is randomized within [delay / 2, delay * 1.5] without exceeding max_delay.
            idempotent: False for calls that must not be repeated once the server may have processed them.
            refresh_token: Callable that obtains a new access token (sync). It receives the token rejected
                by the server, or None if it is unknown. None disables 401 retries.
            arefresh_token: Awaitable counterpart of refresh_token used by async callers.
        """
        if attempts is not None and attempts < 1:
            raise ValueError("attempts must be >= 1 or None")
        if initial_delay < 0 or max_delay < 0 or max_retry_after < 0 or factor < 1:
            raise ValueError("initial_delay, max_delay and max_retry_after must be >= 0, factor must be >= 1")
        self._attempts = attempts
        self.initial_delay = initial_delay
        self.factor = factor
        self.max_delay = max_delay
        self.max_retry_after = max_retry_after
        self.jitter = jitter
        self.idempotent = idempotent
        self.refresh_token = refresh_token
        self.arefresh_token = arefresh_token
        self._max_exponent = self._compute_max_exponent()

    def _compute_max_exponent(self) -> int:
        """Return the retry index from which the un-jittered backoff delay stays at max_delay.

        Returns:
            The smallest retry index whose delay reaches max_delay, or 0 if the delay never grows.
        """
        exponent, delay = 0, self.initial_delay
        while self.factor > 1 and 0 < delay < self.max_delay:
            delay *= self.factor
            exponent += 1
        return exponent

    @property
    def attempts(self) -> int:
        """Return the effective number of attempts, resolved at call time."""
        if self._attempts is None:
            return resolve_max_attempts(MAX_RETRY_ATTEMPTS)
        return min(resolve_max_attempts(self._attempts), self._attempts)

    def delay_for(self, retry_index: int, retry_after: Optional[float] = None) -> float:
        """Return the delay before the given retry.

        Args:
            retry_index: Zero-based index of the retry (0 for the first retry).
            retry_after: Delay suggested by the server, in seconds. The result is never shorter than it,
                unless it exceeds max_retry_after, which is used instead.

        Returns:
            Delay in seconds.
        """
        # The exponent is frozen once the backoff reaches max_delay, so long failure streaks cannot overflow the power
        exponent = min(retry_index, self._max_exponent)
        delay = min(self.max_delay, self.initial_delay * self.factor**exponent)
        if self.jitter:
            delay = min(self.max_delay, random.uniform(delay * 0.5, delay * 1.5))
        if retry_after is not None:
            delay = max(delay, min(retry_after, self.max_retry_after))
        return delay

    def is_retryable(self, exc: BaseException) -> bool:
        """Return True if a call that failed with the given exception may be repeated.

        Fatal errors are never repeated; any other error is treated as transient. Non-idempotent calls are
        repeated only if the server could not have processed them. AuthError is not covered here: it is
        handled separately because it requires a token refresh.

        Args:
            exc: The exception raised by the call.

        Returns:
            True if the call should be repeated, False if the exception must be propagated.
        """
        if isinstance(exc, _FATAL_EXCEPTIONS) or isinstance(exc, AuthError):
            return False
        if isinstance(exc, APIConcurrencyLimitError):
            return True
        if not self.idempotent:
            return _is_connect_phase_error(exc)
        return True

    @staticmethod
    def can_refresh_token(exc: BaseException, auth_token_provided: bool) -> bool:
        """Return True if the exception is a 401 that can be resolved by refreshing the SDK-managed token.

        Args:
            exc: The exception raised by the call.
            auth_token_provided: True if the caller passed its own token, which the SDK cannot regenerate.

        Returns:
            True if a token refresh followed by a repeated call makes sense.
        """
        return isinstance(exc, AuthError) and not isinstance(exc, ForbiddenError) and not auth_token_provided

    def sleep(self, seconds: float) -> None:
        """Block for the given number of seconds. Kept as a method so that tests can patch it.

        Args:
            seconds: How long to wait.
        """
        time.sleep(seconds)

    async def asleep(self, seconds: float) -> None:
        """Asynchronously wait for the given number of seconds. Kept as a method so that tests can patch it.

        Args:
            seconds: How long to wait.
        """
        await asyncio.sleep(seconds)

    def execute(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Call a synchronous function, repeating it according to the policy.

        Args:
            func: The function to call. If it accepts an auth_token keyword argument, a non-empty value
                disables the token refresh on 401.
            *args, **kwargs: Arguments forwarded to func.

        Returns:
            The value returned by func.
        """
        attempts = self.attempts
        auth_token_provided = bool(kwargs.get('auth_token'))
        token_refreshed = False
        attempt = 0
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if self.can_refresh_token(exc, auth_token_provided):
                    if token_refreshed or self.refresh_token is None:
                        raise
                    logger.warning('%s: authorization failed, requesting a new access token', func.__name__)
                    self.refresh_token(getattr(exc, 'rejected_token', None))
                    token_refreshed = True
                    # The refresh corrects the request instead of repeating it, so it does not consume an attempt
                    continue
                attempt += 1
                if attempt >= attempts or not self.is_retryable(exc):
                    raise
                delay = self.delay_for(attempt - 1, getattr(exc, 'retry_after', None))
                logger.warning(
                    '%s failed with %r, retry %s/%s in %.2f seconds',
                    func.__name__,
                    exc,
                    attempt,
                    attempts - 1,
                    delay,
                )
                self.sleep(delay)

    async def aexecute(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        """Await a coroutine function, repeating it according to the policy.

        Args:
            func: The coroutine function to call. If it accepts an auth_token keyword argument, a non-empty
                value disables the token refresh on 401.
            *args, **kwargs: Arguments forwarded to func.

        Returns:
            The value returned by func.
        """
        attempts = self.attempts
        auth_token_provided = bool(kwargs.get('auth_token'))
        token_refreshed = False
        attempt = 0
        while True:
            try:
                return await func(*args, **kwargs)
            except Exception as exc:
                if self.can_refresh_token(exc, auth_token_provided):
                    if token_refreshed or self.arefresh_token is None:
                        raise
                    logger.warning('%s: authorization failed, requesting a new access token', func.__name__)
                    await self.arefresh_token(getattr(exc, 'rejected_token', None))
                    token_refreshed = True
                    # The refresh corrects the request instead of repeating it, so it does not consume an attempt
                    continue
                attempt += 1
                if attempt >= attempts or not self.is_retryable(exc):
                    raise
                delay = self.delay_for(attempt - 1, getattr(exc, 'retry_after', None))
                logger.warning(
                    '%s failed with %r, retry %s/%s in %.2f seconds',
                    func.__name__,
                    exc,
                    attempt,
                    attempts - 1,
                    delay,
                )
                await self.asleep(delay)


def retry(policy: RetryPolicy):
    """Decorate a sync or async function so that it is executed through the given RetryPolicy.

    Args:
        policy: The policy that controls repetitions and delays.

    Returns:
        A decorator that wraps the target function.
    """

    def decorator(func):
        """Wrap the function so that each call goes through the policy.

        Args:
            func: The sync or async function to wrap.

        Returns:
            The wrapped function of the same kind (sync or async).
        """
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                """Await the wrapped coroutine function through RetryPolicy.aexecute.

                Args:
                    *args, **kwargs: Arguments forwarded to the wrapped function.

                Returns:
                    The value returned by the wrapped function.
                """
                return await policy.aexecute(func, *args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            """Call the wrapped function through RetryPolicy.execute.

            Args:
                *args, **kwargs: Arguments forwarded to the wrapped function.

            Returns:
                The value returned by the wrapped function.
            """
            return policy.execute(func, *args, **kwargs)

        return sync_wrapper

    return decorator


def auth_retry_policy() -> RetryPolicy:
    """Create the policy used for OAuth2 token requests: fewer attempts and no token refresh.

    Returns:
        A RetryPolicy instance.
    """
    return RetryPolicy(attempts=AUTH_MAX_RETRY_ATTEMPTS)
