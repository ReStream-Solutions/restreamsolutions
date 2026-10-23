import asyncio
import errno
import json
import ssl

import pandas as pd
import pytest

from restreamsolutions.data_object import Data, DataAsync
from restreamsolutions.exceptions import APICompatibilityError, AuthError, BadRequestError, ServerError
from restreamsolutions.retry import RetryPolicy
from restreamsolutions.site import Site


class ScriptedSource:
    """Factory that replays a script of items and exceptions across generator restarts.

    Each element of the script is a list for one run: items are yielded, exceptions are raised,
    and None ends the run normally.
    """

    def __init__(self, runs):
        """Store the script of runs.

        Args:
            runs: Iterable of runs; each run is a list of items, exceptions and None markers.
        """
        self.runs = list(runs)
        self.started = 0

    def _next_run(self):
        """Start the next run and count it.

        Returns:
            The steps of the next run, or an empty list when the script is exhausted.
        """
        self.started += 1
        return self.runs.pop(0) if self.runs else []

    def sync(self):
        """Replay the next run as a synchronous generator.

        Yields:
            The items of the run until an exception is raised or None ends the run.
        """
        for step in self._next_run():
            if isinstance(step, Exception):
                raise step
            if step is None:
                return
            yield step

    async def asyncgen(self):
        """Replay the next run as an asynchronous generator.

        Yields:
            The items of the run until an exception is raised or None ends the run.
        """
        for step in self._next_run():
            if isinstance(step, Exception):
                raise step
            if step is None:
                return
            yield step


def quiet_policy(**kwargs):
    """Create a policy whose sleeps record delays instead of waiting.

    Args:
        **kwargs: Arguments passed to RetryPolicy.

    Returns:
        The RetryPolicy with a slept list that receives every requested delay.
    """
    policy = RetryPolicy(**kwargs)
    policy.slept = []
    policy.sleep = policy.slept.append

    async def asleep(seconds):
        """Record an asynchronous retry delay instead of sleeping.

        Args:
            seconds: The delay the policy requested.
        """
        policy.slept.append(seconds)

    policy.asleep = asleep
    return policy


def collect(data, limit=None):
    """Consume the data stream, optionally stopping after a number of items (for endless streams).

    Args:
        data: The Data or DataAsync object to consume.
        limit: Maximum number of items to collect, or None to consume the whole stream.

    Returns:
        The list of collected items.
    """
    if isinstance(data, DataAsync):

        async def _collect():
            """Consume the asynchronous data stream up to the limit.

            Returns:
                The list of collected items.
            """
            items = []
            async for item in data.data_fetcher:
                items.append(item)
                if limit is not None and len(items) >= limit:
                    break
            return items

        return asyncio.run(_collect())
    items = []
    for item in data.data_fetcher:
        items.append(item)
        if limit is not None and len(items) >= limit:
            break
    return items


def make_data(source, is_async, **kwargs):
    """Wrap a scripted source into a Data or DataAsync object.

    Args:
        source: The ScriptedSource that provides the generator.
        is_async: Whether to create DataAsync over the async generator instead of Data.
        **kwargs: Arguments passed to the data object constructor.

    Returns:
        The created Data or DataAsync object.
    """
    if is_async:
        return DataAsync(source.asyncgen, **kwargs)
    return Data(source.sync, **kwargs)


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_errors_before_the_first_item_are_retried_and_errors_after_items_are_propagated(is_async):
    """Errors before the first item are retried; later ones are retried only when restart_after_partial is set."""
    policy = quiet_policy()
    source = ScriptedSource([[ServerError()], [ConnectionError()], [{'a': 1}, {'a': 2}]])
    data = make_data(
        source, is_async, restart_on_error=True, attempts=5, retry_policy=policy, restart_after_partial=False
    )
    assert collect(data) == [{'a': 1}, {'a': 2}]
    assert source.started == 3
    assert len(policy.slept) == 2

    # An error after items were delivered is not retried when restart_after_partial is False
    source = ScriptedSource([[{'a': 1}, ServerError()], [{'a': 2}]])
    data = make_data(
        source, is_async, restart_on_error=True, attempts=5, retry_policy=policy, restart_after_partial=False
    )
    with pytest.raises(ServerError):
        collect(data)
    assert source.started == 1

    # The default (restart_after_partial=True) restarts and continues the stream
    source = ScriptedSource([[{'a': 1}, ServerError()], [{'a': 2}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=5, retry_policy=policy)
    assert collect(data) == [{'a': 1}, {'a': 2}]


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_attempts_limit_and_fatal_errors(is_async):
    """Retries stop at the attempts limit, fatal errors are never retried, and invalid attempts raise ValueError."""
    policy = quiet_policy()
    source = ScriptedSource([[ServerError()], [ServerError()], [ServerError()], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=3, retry_policy=policy)
    with pytest.raises(ServerError):
        collect(data)
    assert source.started == 3

    for fatal in (BadRequestError(), APICompatibilityError(), ValueError('x')):
        source = ScriptedSource([[fatal], [{'a': 1}]])
        data = make_data(source, is_async, restart_on_error=True, attempts=None, retry_policy=policy)
        with pytest.raises(type(fatal)):
            collect(data)
        assert source.started == 1

    # Without restart_on_error even transient errors are propagated
    source = ScriptedSource([[ServerError()], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=False, retry_policy=policy)
    with pytest.raises(ServerError):
        collect(data)

    with pytest.raises(ValueError):
        make_data(source, is_async, attempts=0)


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_unknown_errors_trigger_a_reconnect(is_async):
    """Errors that are not known to be fatal (network, TLS, DNS failures, etc.) restart the stream."""
    policy = quiet_policy()
    source = ScriptedSource(
        [[OSError(errno.ENETUNREACH, 'Network is unreachable')], [ssl.SSLEOFError(8, 'EOF')], [{'a': 1}]]
    )
    data = make_data(source, is_async, restart_on_error=True, attempts=None, retry_policy=policy)
    assert collect(data) == [{'a': 1}]
    assert source.started == 3
    assert len(policy.slept) == 2


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_authorization_errors_refresh_the_token_once(is_async):
    """A 401 refreshes the SDK-managed token once, passing the rejected token to the refresh callback."""
    refreshes = []

    async def arefresh(rejected_token):
        """Record an asynchronous token refresh.

        Args:
            rejected_token: The token rejected by the server.
        """
        refreshes.append(rejected_token)

    policy = quiet_policy(refresh_token=refreshes.append, arefresh_token=arefresh)
    source = ScriptedSource([[AuthError(rejected_token='old-token')], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=None, retry_policy=policy)
    assert collect(data) == [{'a': 1}]
    assert refreshes == ['old-token']
    assert policy.slept == []

    # A second consecutive 401 is propagated instead of refreshing again
    refreshes.clear()
    source = ScriptedSource([[AuthError()], [AuthError()], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=None, retry_policy=policy)
    with pytest.raises(AuthError):
        collect(data)
    assert len(refreshes) == 1

    # A user-provided token cannot be refreshed by the SDK
    refreshes.clear()
    source = ScriptedSource([[AuthError()], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=None, retry_policy=policy, auth_token='tok')
    with pytest.raises(AuthError):
        collect(data)
    assert refreshes == []

    # An empty token means the SDK-managed token, which can be refreshed
    source = ScriptedSource([[AuthError()], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=None, retry_policy=policy, auth_token='')
    assert collect(data) == [{'a': 1}]
    assert len(refreshes) == 1

    # The refresh corrects the request instead of repeating it, so it does not consume the single attempt
    refreshes.clear()
    source = ScriptedSource([[AuthError()], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=1, retry_policy=policy)
    assert collect(data) == [{'a': 1}]
    assert len(refreshes) == 1
    source = ScriptedSource([[ServerError()], [{'a': 1}]])
    data = make_data(source, is_async, restart_on_error=True, attempts=1, retry_policy=policy)
    with pytest.raises(ServerError):
        collect(data)


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_restart_on_close_and_model_conversion(is_async):
    """Clean closes reconnect when restart_on_close is set, and items are converted to models with the auth token."""
    policy = quiet_policy()
    source = ScriptedSource([[{'id': 1, 'latitude': 1.5}, None], [{'id': 2, 'latitude': 2.5}, None]])
    data = make_data(source, is_async, restart_on_close=True, convert_to=Site, auth_token='tok', retry_policy=policy)
    # The stream reconnects after every clean close, so the consumer decides when to stop
    items = collect(data, limit=2)
    assert [item.id for item in items] == [1, 2]
    assert all(isinstance(item, Site) and item._auth_token == 'tok' for item in items)
    assert source.started == 2
    assert len(policy.slept) == 1

    # Items that cannot be converted are an API compatibility problem
    source = ScriptedSource([[{'id': 1, 'latitude': 'not-a-number'}]])
    data = make_data(source, is_async, convert_to=Site, retry_policy=policy)
    with pytest.raises(APICompatibilityError):
        collect(data)


@pytest.mark.parametrize('is_async', [False, True], ids=['sync', 'async'])
def test_reconnect_delay_grows_while_the_stream_closes_without_items(is_async):
    """Clean closes without data back off exponentially; a delivered item resets the delay."""
    policy = quiet_policy(jitter=False, initial_delay=1, factor=2, max_delay=10)
    source = ScriptedSource([[None], [None], [{'a': 1}, None], [None], [{'b': 2}]])
    data = make_data(source, is_async, restart_on_close=True, retry_policy=policy)
    assert collect(data, limit=2) == [{'a': 1}, {'b': 2}]
    assert policy.slept == [1, 2, 1, 1]


def test_save_json_and_csv(tmp_path):
    """Data is saved to JSON and CSV files, overwritten only on request, and removed if the stream fails midway."""
    items = [{'timestamp': 1, 'a': 1.5}, {'timestamp': 2, 'a': None}]
    data = Data(lambda: iter(items), convert_to=Site, auth_token='tok')

    json_path = tmp_path / 'out' / 'data.json'
    data.save(str(json_path))
    assert json.loads(json_path.read_text(encoding='utf-8')) == items

    with pytest.raises(FileExistsError):
        data.save(str(json_path))
    data.save(str(json_path), overwrite=True)

    csv_path = tmp_path / 'data.csv'
    data.save(str(csv_path))
    frame = pd.read_csv(csv_path)
    assert list(frame.columns) == ['timestamp', 'a']
    assert len(frame) == 2
    assert not (tmp_path / '.data.json').exists()

    with pytest.raises(ValueError):
        data.save(str(tmp_path / 'data.txt'))

    # A failure in the middle of the stream removes the partially written file
    failing = Data(ScriptedSource([[{'a': 1}, ServerError()]]).sync)
    with pytest.raises(ServerError):
        failing.save(str(tmp_path / 'partial.json'))
    assert not (tmp_path / 'partial.json').exists()


async def test_async_save_json_and_csv(tmp_path):
    """DataAsync is saved to JSON and CSV files, and a partially written file is removed when the stream fails."""
    items = [{'timestamp': 1, 'a': 1.5}, {'timestamp': 2, 'a': 2.5}]

    async def source():
        """Yield the test items asynchronously.

        Yields:
            The items of the test stream.
        """
        for item in items:
            yield item

    data = DataAsync(source)

    json_path = tmp_path / 'data.json'
    await data.asave(str(json_path))
    assert json.loads(json_path.read_text(encoding='utf-8')) == items

    csv_path = tmp_path / 'data.csv'
    await data.asave(str(csv_path))
    assert len(pd.read_csv(csv_path)) == 2

    failing = DataAsync(ScriptedSource([[{'a': 1}, ServerError()]]).asyncgen)
    with pytest.raises(ServerError):
        await failing.asave(str(tmp_path / 'partial.json'))
    assert not (tmp_path / 'partial.json').exists()
