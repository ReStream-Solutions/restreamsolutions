"""In-process fake of the ReStream third-party API used by the test suite.

The server runs a real aiohttp application in a background thread with its own event loop, so both
the synchronous (requests, websocket-client) and the asynchronous (aiohttp) SDK clients talk to it
over real TCP connections. Responses are built from the JSON fixtures in tests/mock_responses.
"""

import asyncio
import json
import threading
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from aiohttp import web

MOCK_RESPONSES = Path(__file__).resolve().parent.parent / 'mock_responses'
API_PREFIX = '/external/thirdparty/v1'
WS_PREFIX = '/external/thirdparty/v2/ws'
TOKEN_PATH = '/o/token/'
TEST_STREAM_PATH = '/test/stream/'

# Stage aggregation type codes used by the fixture and by the create handler
DEFAULT_AGGREGATION_TYPE = 'D'
CUSTOM_AGGREGATION_TYPE = 'C'


def load_fixture(relative_path: str) -> Any:
    """Load a JSON fixture from tests/mock_responses.

    Args:
        relative_path: Path relative to the mock_responses directory, e.g. 'site/site_one.json'.

    Returns:
        The parsed JSON value.
    """
    with open(MOCK_RESPONSES / relative_path, 'r', encoding='utf-8') as f:
        return json.load(f)


@dataclass
class RecordedRequest:
    """An HTTP request received by the fake server."""

    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    body: Any = None

    def param(self, name: str) -> Optional[str]:
        """Return the first value of a query parameter.

        Args:
            name: Query parameter name.

        Returns:
            The first value of the parameter, or None if it is absent.
        """
        values = self.query.get(name)
        return values[0] if values else None

    @property
    def bearer_token(self) -> Optional[str]:
        """Return the bearer token sent in the Authorization header, if any."""
        value = self.headers.get('Authorization', '')
        return value.split(' ', 1)[1] if value.startswith('Bearer ') else None


@dataclass
class RecordedWebSocket:
    """A WebSocket connection accepted by the fake server."""

    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    received: list[Any] = field(default_factory=list)

    def param(self, name: str) -> Optional[str]:
        """Return the first value of a query parameter.

        Args:
            name: Query parameter name.

        Returns:
            The first value of the parameter, or None if it is absent.
        """
        values = self.query.get(name)
        return values[0] if values else None


class FakeRestreamServer:
    """Programmable fake of the ReStream API.

    Behaviour that tests can control:
    - credentials: client_id -> client_secret pairs accepted by the token endpoint.
    - plan(): queue of canned responses (status or dict) returned for a method/path before normal handling.
    - delays: path -> seconds to wait before handling a request.
    - stream_drop_after: path -> number of items after which a streaming response is cut off.
    - stream_items: path -> list of items streamed instead of the fixture.
    - forbidden_ids / missing_ids: object ids that produce 403 / 404.
    - ws_scripts: path -> list of messages sent to a WebSocket client right after the handshake.
    - ws_close_after_script: close the WebSocket after the script, otherwise keep it open until the client leaves.
    """

    DEFAULT_CLIENT_ID = 'test-client-id'
    DEFAULT_CLIENT_SECRET = 'test-client-secret'

    def __init__(self) -> None:
        """Create a stopped server with the default state; call start to run it."""
        self.port: Optional[int] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._stop_event: Optional[asyncio.Event] = None
        self.reset()

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Restore the default state between tests."""
        self.credentials: dict[str, str] = {self.DEFAULT_CLIENT_ID: self.DEFAULT_CLIENT_SECRET}
        self.valid_tokens: set[str] = set()
        self.token_requests = 0
        self.requests: list[RecordedRequest] = []
        self.websockets: list[RecordedWebSocket] = []
        self.planned: dict[tuple[str, str], deque] = {}
        self.delays: dict[str, float] = {}
        self.stream_drop_after: dict[str, int] = {}
        self.stream_items: dict[str, list] = {
            TEST_STREAM_PATH: [{'a': 1.5, 'b': 'Infinity'}, {'a': 2, 'b': 'ok'}],
        }
        self.forbidden_ids: set[int] = set()
        self.missing_ids: set[int] = set()
        self.ws_scripts: dict[str, list] = {}
        self.ws_close_after_script = True
        self.acknowledged_change_logs: set[int] = set()
        self.stages_aggregations: dict[int, dict] = {
            item['id']: dict(item) for item in load_fixture('site/stages_aggregations.json')
        }
        self.next_aggregation_id = 1000

    def plan(self, method: str, path: str, *responses: int | dict) -> None:
        """Queue canned responses for a method and path.

        Args:
            method: HTTP method, e.g. 'GET'.
            path: Request path without the host, e.g. '/external/thirdparty/v1/sites/1112/'.
            *responses: Each item is either an HTTP status code or a dict with optional keys
                status, body (JSON-serializable or str) and headers.
        """
        self.planned.setdefault((method.upper(), path), deque()).extend(responses)

    def count(self, method: str, path: str) -> int:
        """Return how many requests with the given method and path were received.

        Args:
            method: HTTP method, case-insensitive.
            path: Request path without the host.

        Returns:
            The number of matching recorded requests.
        """
        return sum(1 for r in self.requests if r.method == method.upper() and r.path == path)

    def last(self, method: str, path: str) -> RecordedRequest:
        """Return the most recent request with the given method and path.

        Args:
            method: HTTP method, case-insensitive.
            path: Request path without the host.

        Returns:
            The last matching recorded request.

        Raises:
            AssertionError: If no matching request was received.
        """
        matching = [r for r in self.requests if r.method == method.upper() and r.path == path]
        if not matching:
            raise AssertionError(f'No {method} request to {path} was received')
        return matching[-1]

    def revoke_tokens(self) -> None:
        """Invalidate every issued token so that the next API call gets HTTP 401."""
        self.valid_tokens.clear()

    @property
    def base_url(self) -> str:
        """Return the HTTP base URL of the running server."""
        return f'http://127.0.0.1:{self.port}'

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the server in a background thread and wait until it accepts connections."""
        self._thread = threading.Thread(target=self._run, name='fake-restream-api', daemon=True)
        self._thread.start()
        if not self._ready.wait(10):
            raise RuntimeError('The fake API server did not start in time')

    def stop(self) -> None:
        """Stop the server and wait for the background thread to finish."""
        if self._loop is not None and self._stop_event is not None:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread is not None:
            self._thread.join(10)

    def _run(self) -> None:
        """Run the server event loop in the background thread until stop is called."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._stop_event = asyncio.Event()
        self._loop.run_until_complete(self._serve())
        self._loop.close()

    async def _serve(self) -> None:
        """Serve the application on a free local port until the stop event is set.

        The chosen port is stored in port and the ready event is set once the server accepts connections.
        """
        runner = web.AppRunner(self._build_app())
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        self.port = runner.addresses[0][1]
        self._ready.set()
        await self._stop_event.wait()
        await runner.cleanup()

    # ------------------------------------------------------------------
    # Application
    # ------------------------------------------------------------------

    def _build_app(self) -> web.Application:
        """Build the aiohttp application with the token, test stream, site, pad, state and WebSocket routes.

        Returns:
            The configured aiohttp application.
        """
        app = web.Application(middlewares=[self._control_middleware])
        app.router.add_post(TOKEN_PATH, self._token)
        app.router.add_get(TEST_STREAM_PATH, self._test_stream)

        for kind in ('sites', 'pads'):
            prefix = f'{API_PREFIX}/{kind}'
            app.router.add_get(f'{prefix}/', self._list_objects)
            app.router.add_get(f'{prefix}/{{id}}/', self._get_object)
            app.router.add_get(f'{prefix}/{{id}}/fields/', self._fields)
            app.router.add_get(f'{prefix}/{{id}}/history/', self._history)
            app.router.add_get(f'{prefix}/{{id}}/aggregations/', self._aggregations)
            app.router.add_get(f'{prefix}/{{id}}/data/', self._data)
            app.router.add_get(f'{prefix}/{{id}}/data_changes/', self._data_changes)
            app.router.add_post(f'{prefix}/{{id}}/data_changes/', self._acknowledge_data_changes)
            app.router.add_get(f'{prefix}/{{id}}/parameters/', self._list_aggregations)
            app.router.add_post(f'{prefix}/{{id}}/parameters/', self._create_aggregations)
            app.router.add_get(f'{prefix}/{{id}}/parameters/{{aggregation_id}}/', self._get_aggregation)
            app.router.add_patch(f'{prefix}/{{id}}/parameters/{{aggregation_id}}/', self._update_aggregation)
            app.router.add_put(f'{prefix}/{{id}}/parameters/{{aggregation_id}}/', self._update_aggregation)
            app.router.add_delete(f'{prefix}/{{id}}/parameters/{{aggregation_id}}/', self._delete_aggregation)

            ws_prefix = f'{WS_PREFIX}/{kind}'
            app.router.add_get(f'{ws_prefix}/{{id}}/', self._websocket)
            app.router.add_get(f'{ws_prefix}/{{id}}/data_changes/', self._websocket)
            app.router.add_get(f'{ws_prefix}/{{id}}/data/', self._websocket)

        app.router.add_get(f'{API_PREFIX}/states/', self._states)
        app.router.add_get(f'{API_PREFIX}/states/{{id}}/', self._state)
        return app

    @web.middleware
    async def _control_middleware(self, request: web.Request, handler):
        """Record the request and apply the configured delay, planned response and token check.

        Args:
            request: The incoming request.
            handler: The route handler called when no planned response or token error applies.

        Returns:
            The next planned response, HTTP 401 (HTTP 403 for WebSocket paths) for a missing or invalid token,
            or the handler response.
        """
        path = request.path
        delay = self.delays.get(path)
        if delay:
            await asyncio.sleep(delay)

        raw_body = await request.read()
        body: Any = None
        if raw_body:
            if request.content_type == 'application/json':
                body = json.loads(raw_body)
            elif request.content_type == 'application/x-www-form-urlencoded':
                body = dict(await request.post())
            else:
                body = raw_body.decode('utf-8', errors='replace')
        self.requests.append(
            RecordedRequest(
                method=request.method,
                path=path,
                query={key: request.query.getall(key) for key in request.query.keys()},
                headers=dict(request.headers),
                body=body,
            )
        )

        queue = self.planned.get((request.method, path))
        if queue:
            return self._planned_response(queue.popleft())

        if path != TOKEN_PATH:
            token = self._bearer_token(request)
            if token not in self.valid_tokens:
                if path.startswith(WS_PREFIX):
                    # Django Channels rejects a WebSocket handshake closed before accept() with HTTP 403
                    return web.json_response({'detail': 'Token is not provided or not valid'}, status=403)
                return web.json_response({'detail': 'Authentication credentials were not provided.'}, status=401)
        return await handler(request)

    @staticmethod
    def _planned_response(planned: int | dict) -> web.Response:
        """Build a response from a queued plan item.

        Args:
            planned: An HTTP status code or a dict with optional keys status, body and headers (see plan).

        Returns:
            The JSON response; a str body is sent as is with the JSON content type.
        """
        if isinstance(planned, int):
            return web.json_response({'detail': f'planned response {planned}'}, status=planned)
        status = planned.get('status', 200)
        body = planned.get('body', {'detail': f'planned response {status}'})
        headers = planned.get('headers')
        if isinstance(body, str):
            return web.Response(text=body, status=status, headers=headers, content_type='application/json')
        return web.json_response(body, status=status, headers=headers)

    @staticmethod
    def _bearer_token(request: web.Request) -> Optional[str]:
        """Extract the bearer token from the Authorization header.

        Args:
            request: The incoming request.

        Returns:
            The token, or None if the header is missing or is not a bearer one.
        """
        value = request.headers.get('Authorization', '')
        return value.split(' ', 1)[1] if value.startswith('Bearer ') else None

    @staticmethod
    def _kind(request: web.Request) -> str:
        """Tell whether the request targets a site or a pad endpoint.

        Args:
            request: The incoming request.

        Returns:
            'site' for site paths, 'pad' otherwise.
        """
        return 'site' if '/sites/' in request.path else 'pad'

    def _object_error(self, request: web.Request) -> Optional[web.Response]:
        """Return the configured error response for the object id in the request path.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            HTTP 404 for an id in missing_ids, HTTP 403 for an id in forbidden_ids, otherwise None.
        """
        object_id = int(request.match_info['id'])
        if object_id in self.missing_ids:
            return web.json_response({'detail': 'Not found.'}, status=404)
        if object_id in self.forbidden_ids:
            return web.json_response({'detail': 'You do not have permission to perform this action.'}, status=403)
        return None

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def _token(self, request: web.Request) -> web.Response:
        """Issue a new access token for valid client credentials.

        Args:
            request: The form-encoded token request.

        Returns:
            The token response, or HTTP 401 for a grant type other than client_credentials or wrong credentials.
        """
        form = await request.post()
        client_id = form.get('client_id')
        if form.get('grant_type') != 'client_credentials' or self.credentials.get(client_id) != form.get(
            'client_secret'
        ):
            return web.json_response({'error': 'invalid_client'}, status=401)
        self.token_requests += 1
        token = f'{client_id}-token-{self.token_requests}'
        self.valid_tokens.add(token)
        return web.json_response({'access_token': token, 'expires_in': 3600, 'token_type': 'Bearer', 'scope': 'read'})

    async def _list_objects(self, request: web.Request) -> web.Response:
        """Return the list fixture of sites or pads.

        Args:
            request: The incoming request.

        Returns:
            The JSON response with the list of objects.
        """
        fixture = 'site/sites_many.json' if self._kind(request) == 'site' else 'pad/pad_many.json'
        return web.json_response(load_fixture(fixture))

    async def _get_object(self, request: web.Request) -> web.Response:
        """Return the site or pad fixture with its id replaced by the requested one.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            The JSON response with the object, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        fixture = 'site/site_one.json' if self._kind(request) == 'site' else 'pad/pad_one.json'
        payload = load_fixture(fixture)
        payload['id'] = int(request.match_info['id'])
        return web.json_response(payload)

    async def _states(self, request: web.Request) -> web.Response:
        """Return the states fixture of a site if site__id is requested, otherwise the states fixture of a pad.

        Args:
            request: The incoming request.

        Returns:
            The JSON response with the list of states.
        """
        fixture = 'site/states_many_by_site.json' if 'site__id' in request.query else 'pad/states_many_by_pad.json'
        return web.json_response(load_fixture(fixture))

    async def _state(self, request: web.Request) -> web.Response:
        """Return the first pad state fixture with its id replaced by the requested one.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            The JSON response with the state.
        """
        payload = load_fixture('pad/states_many_by_pad.json')[0]
        payload['id'] = int(request.match_info['id'])
        return web.json_response(payload)

    async def _fields(self, request: web.Request) -> web.Response:
        """Return the fields fixture of a site or a pad.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            The JSON response with the fields, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        fixture = 'site/fields_site.json' if self._kind(request) == 'site' else 'pad/fields_pad.json'
        return web.json_response(load_fixture(fixture))

    async def _history(self, request: web.Request) -> web.Response:
        """Return the stages fixture of a site or a pad, the filtered one if stage_number or stage_numbers is requested.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            The JSON response with the stages, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        filtered = 'stage_number' in request.query or 'stage_numbers' in request.query
        kind = self._kind(request)
        fixture = f'{kind}/stages_{kind}{"_filtered" if filtered else ""}.json'
        return web.json_response(load_fixture(fixture))

    async def _aggregations(self, request: web.Request) -> web.Response:
        """Return the aggregations fixture of a site or a pad.

        Args:
            request: The incoming request with an id path parameter and a histories or from_range filter.

        Returns:
            The JSON response with the aggregations, HTTP 400 without a time filter, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        if 'histories' not in request.query and 'from_range' not in request.query:
            return web.json_response({'status': 'Must request a time filter'}, status=400)
        kind = self._kind(request)
        return web.json_response(load_fixture(f'{kind}/aggregations_{kind}.json'))

    async def _data(self, request: web.Request) -> web.StreamResponse:
        """Stream the measurements of a site or a pad as a JSON array.

        The items configured in stream_items for the request path are streamed instead of the fixture if present.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            The streaming response, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        kind = self._kind(request)
        items = self.stream_items.get(request.path, load_fixture(f'{kind}/data_{kind}.json'))
        return await self._stream_json_array(request, items)

    async def _test_stream(self, request: web.Request) -> web.StreamResponse:
        """Stream the items configured in stream_items for TEST_STREAM_PATH as a JSON array.

        Args:
            request: The incoming request.

        Returns:
            The streaming response.
        """
        return await self._stream_json_array(request, self.stream_items[TEST_STREAM_PATH])

    async def _stream_json_array(self, request: web.Request, items: list) -> web.StreamResponse:
        """Write the items as a JSON array, sending each item in a separate chunk.

        If stream_drop_after is set for the request path, the connection is closed abruptly after that many items.

        Args:
            request: The incoming request.
            items: JSON-serializable items to stream.

        Returns:
            The streaming response.
        """
        response = web.StreamResponse(status=200, headers={'Content-Type': 'application/json'})
        await response.prepare(request)
        await response.write(b'[')
        drop_after = self.stream_drop_after.get(request.path)
        for index, item in enumerate(items):
            if drop_after is not None and index >= drop_after:
                # Simulate a connection drop in the middle of the stream
                request.transport.close()
                return response
            if index:
                await response.write(b',')
            await response.write(json.dumps(item).encode('utf-8'))
        await response.write(b']')
        await response.write_eof()
        return response

    def _change_log_fixture(self, request: web.Request) -> list[dict]:
        """Load the change log fixture of a site or a pad.

        Args:
            request: The incoming request.

        Returns:
            The list of change log entries.
        """
        kind = self._kind(request)
        return load_fixture(f'{kind}/data_changes_{kind}_many.json')['change_log']

    async def _data_changes(self, request: web.Request) -> web.Response:
        """Return the change log, without the acknowledged entries unless get_acknowledged is true.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            The JSON response with the change_log list, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        entries = self._change_log_fixture(request)
        if request.query.get('get_acknowledged', '').lower() != 'true':
            entries = [e for e in entries if e['id'] not in self.acknowledged_change_logs]
        return web.json_response({'change_log': entries})

    async def _acknowledge_data_changes(self, request: web.Request) -> web.Response:
        """Mark the change log entries listed in the request body as acknowledged.

        Args:
            request: The incoming request with an id path parameter and a JSON body whose change_log items have
                an integer id and update_received set to true.

        Returns:
            The JSON response with the remaining unacknowledged entries, HTTP 400 for an invalid item,
            or the configured error response.
        """
        if error := self._object_error(request):
            return error
        body = await request.json()
        updates = body.get('change_log') or []
        for update in updates:
            if not isinstance(update.get('id'), int) or update.get('update_received') is not True:
                return web.json_response({'detail': 'Invalid change_log item.'}, status=400)
            self.acknowledged_change_logs.add(update['id'])
        entries = [e for e in self._change_log_fixture(request) if e['id'] not in self.acknowledged_change_logs]
        return web.json_response({'change_log': entries})

    async def _list_aggregations(self, request: web.Request) -> web.Response:
        """Return all stage aggregations stored in the server state.

        Args:
            request: The incoming request with an id path parameter.

        Returns:
            The JSON response with the list of aggregations, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        return web.json_response(list(self.stages_aggregations.values()))

    async def _create_aggregations(self, request: web.Request) -> web.Response:
        """Validate the JSON array of new stage aggregations and store them as custom aggregations.

        With validate_only set to true the body is only validated and nothing is stored.

        Args:
            request: The incoming request with an id path parameter and a JSON array body.

        Returns:
            HTTP 201 with the created aggregations, a valid flag for validate_only, HTTP 400 with the validation
            errors, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        body = await request.json()
        if not isinstance(body, list):
            return web.json_response({'non_field_errors': ['Request body must be a JSON array.']}, status=400)
        errors = {}
        for index, item in enumerate(body):
            if not isinstance(item, dict) or not item.get('name') or not item.get('config'):
                errors[str(index)] = ['Name and config are required.']
        if errors:
            return web.json_response(errors, status=400)
        if request.query.get('validate_only', '').lower() == 'true':
            return web.json_response({'valid': True})
        created = []
        for item in body:
            self.next_aggregation_id += 1
            aggregation = {
                'id': self.next_aggregation_id,
                'name': item['name'],
                'config': item['config'],
                'description': item.get('description', ''),
                'type': CUSTOM_AGGREGATION_TYPE,
                'imperial_unit': None,
                'si_unit': None,
            }
            self.stages_aggregations[aggregation['id']] = aggregation
            created.append(aggregation)
        return web.json_response(created, status=201)

    def _find_aggregation(self, request: web.Request) -> dict | web.Response:
        """Find a stored stage aggregation by the aggregation_id path parameter.

        Args:
            request: The incoming request with an aggregation_id path parameter.

        Returns:
            The stored aggregation dict, or HTTP 404 if it does not exist.
        """
        aggregation = self.stages_aggregations.get(int(request.match_info['aggregation_id']))
        if aggregation is None:
            return web.json_response({'detail': 'Not found.'}, status=404)
        return aggregation

    async def _get_aggregation(self, request: web.Request) -> web.Response:
        """Return a single stage aggregation.

        Args:
            request: The incoming request with id and aggregation_id path parameters.

        Returns:
            The JSON response with the aggregation, HTTP 404 if it does not exist, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        aggregation = self._find_aggregation(request)
        if isinstance(aggregation, web.Response):
            return aggregation
        return web.json_response(aggregation)

    async def _update_aggregation(self, request: web.Request) -> web.Response:
        """Update the config, description, label and color of a custom stage aggregation.

        PATCH merges the given fields, PUT resets these fields before merging. Default aggregations cannot be changed.

        Args:
            request: The incoming request with id and aggregation_id path parameters and a JSON body.

        Returns:
            The JSON response with the updated aggregation, HTTP 400 for a default aggregation, HTTP 404 if it does
            not exist, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        aggregation = self._find_aggregation(request)
        if isinstance(aggregation, web.Response):
            return aggregation
        if aggregation.get('type') == DEFAULT_AGGREGATION_TYPE:
            return web.json_response(['Default pad parameters cannot be modified via this API.'], status=400)
        body = await request.json()
        if request.method == 'PUT':
            aggregation.update({'config': None, 'description': '', 'label': '', 'color': None})
        aggregation.update(
            {key: value for key, value in body.items() if key in ('config', 'description', 'label', 'color')}
        )
        return web.json_response(aggregation)

    async def _delete_aggregation(self, request: web.Request) -> web.Response:
        """Delete a custom stage aggregation. Default aggregations cannot be deleted.

        Args:
            request: The incoming request with id and aggregation_id path parameters.

        Returns:
            HTTP 204 on success, HTTP 400 for a default aggregation, HTTP 404 if it does not exist,
            or the configured error response.
        """
        if error := self._object_error(request):
            return error
        aggregation = self._find_aggregation(request)
        if isinstance(aggregation, web.Response):
            return aggregation
        if aggregation.get('type') == DEFAULT_AGGREGATION_TYPE:
            return web.json_response(['Default pad parameters cannot be modified via this API.'], status=400)
        del self.stages_aggregations[aggregation['id']]
        return web.Response(status=204)

    async def _websocket(self, request: web.Request) -> web.StreamResponse:
        """Accept a WebSocket connection, record it and send the messages scripted in ws_scripts for its path.

        A str message is sent as is; other messages are sent as JSON, wrapped with the session key on data streams.
        Afterwards the socket is closed if ws_close_after_script is set, otherwise incoming messages are recorded
        until the client disconnects.

        Args:
            request: The WebSocket handshake request with an id path parameter.

        Returns:
            The WebSocket response, or the configured error response.
        """
        if error := self._object_error(request):
            return error
        record = RecordedWebSocket(
            path=request.path,
            query={key: request.query.getall(key) for key in request.query.keys()},
            headers=dict(request.headers),
        )
        self.websockets.append(record)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        is_data_stream = request.path.endswith('/data/')
        for message in self.ws_scripts.get(request.path, []):
            if isinstance(message, str):
                await ws.send_str(message)
            elif is_data_stream:
                await ws.send_json({'session_key': record.param('session_key'), 'message': message})
            else:
                await ws.send_json(message)
        if self.ws_close_after_script:
            await ws.close()
        else:
            async for incoming in ws:
                record.received.append(incoming.data)
        return ws
