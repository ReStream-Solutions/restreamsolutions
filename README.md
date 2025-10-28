# ReStream Datastore SDK

This project provides a convenient Python SDK for interacting with the Restream API. Using this SDK, you can:
- check the configuration of your Sites and Pads and receive updates in real time
- get information about the current and historical stages of the Sites with additional aggregated metrics
- retrieve raw or sampled data from your Sites and Pads
- get a list of changes that were applied to the data, download this data, and confirm that these changes were received
- connect to a WebSocket room associated with a specific Site or Pad and receive new data in real time

## Installation and Build

Install from PyPI:

```bash
pip install restreamsolutions
```

Build a wheel package from sources:

```bash
# Requires Poetry to be installed
# Run from the repository root
./build.sh
```

Where to find the .whl file:
- After running the script, the built artifact will be in the `dist/` directory.
- The name will look like: `restreamsolutions-<version>-py3-none-any.whl`.

Use in another project (local installation from the .whl file):

```bash
# From the other project's directory
pip install /full/path/to/restreamsolutions-<version>-py3-none-any.whl
```

Install for local development from sources (optional):

```bash
# Requires Poetry to be installed
poetry install
```

## Setup

Provide your ReStream OAuth2 client credentials via environment variables RESTREAM_CLIENT_ID and RESTREAM_CLIENT_SECRET.
The SDK will automatically request and cache an access token when needed.
You can also pass an access token directly to the SDK classes and methods if you already have one.

```python
import os
from restreamsolutions import Pad

# Preferred: set client credentials as environment variables
os.environ["RESTREAM_CLIENT_ID"] = "your_client_id"
os.environ["RESTREAM_CLIENT_SECRET"] = "your_client_secret"

# Now you can call SDK methods without passing a token — it will be obtained automatically
pads = Pad.get_models()

# If you already have a token, you can still pass it directly to methods
pads = Pad.get_models(auth_token="your token")
```

## Using the Authorization class

You can use the Authorization helper to request and cache an access token using your client credentials. This is useful when you need a token outside of the SDK calls (for example, to paste into Swagger UI or other tools).

```python
import os
from restreamsolutions import Authorization, Pad, Site

# Option 1: use environment variables (recommended)
os.environ["RESTREAM_CLIENT_ID"] = "your_client_id"
os.environ["RESTREAM_CLIENT_SECRET"] = "your_client_secret"

# Get and cache an access token
auth = Authorization()
token = auth.get_access_token()

# You can pass the token to SDK methods or constructors explicitly if you prefer
pads = Pad.get_models(auth_token=token)
site = Site(id=123, auth_token=token)
states = site.get_states()

# Option 2: provide credentials directly (overrides env vars)
other_token = auth.get_access_token(client_id="id", client_secret="secret", force=True)

# Force refresh the token when needed
fresh_token = auth.get_access_token(force=True)
```

Note: If you set RESTREAM_CLIENT_ID/RESTREAM_CLIENT_SECRET, the SDK will obtain tokens automatically when you call methods. Passing auth_token directly remains fully supported in both class constructors and methods.

## Usage

You will primarily interact with two classes, `Site` and `Pad`, which provide access to all the information available
through the Restream API. Configure authentication as shown above (via RESTREAM_CLIENT_ID/RESTREAM_CLIENT_SECRET) or pass
an access token directly to methods/constructors when needed. In the examples, this step is omitted for brevity.

### Fetching Sites and Pads

#### Get a list of all models

You can retrieve a list of all `Pads` or `Sites` available to you.

By default, the SDK returns a list of class objects (Pad or Site). During serialization, all fields from the HTTP
response become attributes of the class. Fields containing timestamps are automatically converted to timezone-aware
Python `datetime` objects.

```python
from restreamsolutions import Pad, Site

# Get a list of all pads as Pad objects
pads = Pad.get_models()
for pad in pads:
    print(f'pad id: {pad.id}, pad name: {pad.name}, simops config: {pad.simops_config}')

# Similarly, for sites
sites = Site.get_models()
```

Alternatively, you can get the raw response from the endpoint as a list of Python `dict` objects by 
setting `as_dict=True`.

```python
# Get a list of all pads as dicts
from restreamsolutions import Pad, Site

pads_as_dict = Pad.get_models(as_dict=True)
for pad in pads_as_dict:
    print(f'pad id: {pad["id"]}, pad name: {pad["name"]}, simops config: {pad["simops_config"]}')

# Similarly, for sites
sites_as_dict = Site.get_models(as_dict=True)
```

#### Get a single model by ID

If you know the ID of a `Site` or `Pad`, you can fetch a specific object using the `get_model` class method.

```python
from restreamsolutions import Pad, Site

# Get a specific Pad object by its ID
pad = Pad.get_model(id=668)

# Get a specific Site object by its ID
site = Site.get_model(id=981)

# You can also get the model as a dict
pad_as_dict = Pad.get_model(id=668, as_dict=True)
site_as_dict = Site.get_model(id=981, as_dict=True)
```

#### Working with object instances vs. dictionaries

You can instantiate a class object directly from a dictionary. The class constructor will assign all attributes and
perform conversions, such as turning date strings into `datetime` objects.

```python
from restreamsolutions import Site

site_as_dict = Site.get_model(id=981, as_dict=True)
site = Site(**site_as_dict)
print(f'date as datetime: {site.date_created}')
print(f'date as a string: {site_as_dict["date_created"]}')
```

### Working with Object Relationships

#### Getting child objects (Sites from a Pad)

To get all `Sites` belonging to a `Pad`, you can use the `get_sites()` method. For efficiency, it's better to 
instantiate the `Pad` with just its ID and then call the method.

```python
from restreamsolutions import Pad

# This approach makes two API calls: one to get the Pad, another to get its Sites.
sites_v1 = Pad.get_model(id=668).get_sites()

# This is more efficient, making only one API call to get the Sites.
sites_v2 = Pad(id=668).get_sites()
``` 

When you create an object with only an `id`, it won't have other attributes populated. You can still use its instance 
methods to fetch related data. To load the object's own attributes, use the `update()` or `aupdate()` (async) method.

```python
from restreamsolutions import Pad

pad = Pad(id=668)
print(f"The API call to get the pad has not been made yet. Pad name: {getattr(pad, 'name', None)}")

# But we can still call any instance method
sites = pad.get_sites()
print(f'Successfully fetched sites: {sites}')

# Now, load the pad's own attributes
pad.update()
print(f"The update() method made an API call and loaded all attributes. Pad name: {pad.name}")
```

#### Getting parent objects (Pad from a Site)

To get the parent `Pad` for a `Site`, use the `get_pad()` instance method. The `as_dict` parameter is also available.

```python
from restreamsolutions import Site

pad = Site(id=981).get_pad()
pad_as_dict = Site(id=981).get_pad(as_dict=True)
```

### Getting and Monitoring Site State

#### Getting the current state

To get the current configuration of a specific site or all sites on a pad, use the `get_state()` 
and `get_states()` methods.

```python
from restreamsolutions import Site, Pad

pad = Pad(id=681)
site = Site(id=981)

# Returns a single State object or None if the site is not configured
site_state = site.get_state()

# Returns a list of State objects for each configured site on the pad
pad_states = pad.get_states()

# You can also use the as_dict parameter
site_state_json = site.get_state(as_dict=True)
pad_states_json = pad.get_states(as_dict=True)
```

#### Filtering states

You can filter the State objects by stage name using `StageNameFilters`.

```python
from restreamsolutions import Pad, StageNameFilters

pad = Pad(id=668)
frac_states = pad.get_states(stage_name_filter=StageNameFilters.FRAC)
```

#### Monitoring state for real-time updates

Since the current State of a site changes, you can monitor it by calling the `update()` method on the state object.
However, there's a better approach — see the “Real-time Sites and Pads updates” section below.

```python
import time
from restreamsolutions import Site, StageNameFilters

state = Site(id=981).get_state()
if state:
    for _ in range(3):
        print(f'State: {state.current_state}, '
              f'Stage number: {state.calculated_stage_number}, '
              f'Last update: {state.last_state_update}')
        time.sleep(60)
        state.update()  # Refresh the state object with the latest data
```

### Getting Historical Stages Data

The `get_stages_metadata()` and `aget_stages_metadata()` (async) methods allow you to retrieve information about 
previous stages with optional filters.

```python
from datetime import datetime, timezone
from restreamsolutions import Site, StageNameFilters

site = Site(id=1113)
start_date = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
end_date = datetime(2025, 10, 18, 0, 0, 0, tzinfo=timezone.utc)

# Get stages within a date range
stages_from_range = site.get_stages_metadata(start=start_date, end=end_date)
for stage in stages_from_range[:3]:
    print(f"ID: {stage['id']}, State: {stage['state']}, Start: {stage['start']}")

# Get stages with a specific stage number and stage name
stages_by_number = site.get_stages_metadata(stage_number=1, stage_name_filter=StageNameFilters.WIRELINE)
```

You can also include aggregated metrics for each stage by setting `add_aggregations=True`.

```python
from datetime import datetime, timezone
from restreamsolutions import Site, StageNameFilters

site = Site(id=1113)
start_date = datetime(2025, 9, 17, 0, 0, 0, tzinfo=timezone.utc)
end_date = datetime(2025, 9, 18, 0, 0, 0, tzinfo=timezone.utc)
stages_with_aggregations = site.get_stages_metadata(
    start_date=start_date,
    end_date=end_date,
    stage_name_filter=StageNameFilters.FRAC,
    add_aggregations=True
)
for stage in stages_with_aggregations[:3]:
    print(f"ID: {stage['id']}, Aggregations: {stage['aggregations']}")
```

### Getting Metadata

#### Measurement Sources Metadata

Retrieve metadata about measurement sources for an entire pad or a specific site.

```python
from restreamsolutions import Pad, Site

# For an entire pad
measurement_sources = Pad(id=681).get_measurement_sources_metadata()
print(f'Pad measurement sources: {measurement_sources}')

# For a specific site
site = Site(id=1111)
measurement_sources = site.get_measurement_sources_metadata()
print(f'Site measurement sources: {measurement_sources}')
```

#### Fields Metadata

Get a list of available data field names for a pad or site. These names can be used to filter data retrieval.

```python
from restreamsolutions import Pad, Site

pad = Pad(id=681)
pad_fields = pad.get_fields_metadata()
print(f'Pad fields: {pad_fields}')

site = Site(id=981)
site_fields = site.get_fields_metadata()
print(f'Site fields: {site_fields}')
```

### Fetching Time-Series Data

To get data for a pad or site, use the `get_data()` or `aget_data()` (async) method. These methods return lazy `Data` or
`DataAsync` objects, which can be used for streaming or saving to a file.

For pads, the `is_routed` parameter (default `False`) controls whether the data is distributed by their specific 
sites (`True`) or returned for the entire pad (`False`). See the `get_data()` documentation for more details.

```python
from datetime import datetime, timezone
from restreamsolutions import Pad, StageNameFilters

pad = Pad(id=681)

data_obj = pad.get_data(
    start_datetime=datetime(2025, 9, 9, tzinfo=timezone.utc),
    end_datetime=datetime(2025, 9, 9, minute=1, tzinfo=timezone.utc),
    is_routed=True,
    stage_name_filter=StageNameFilters.FRAC
)
```

#### Streaming data

Data begins to download the first time you access the `data_fetcher` generator, 
allowing you to process it immediately without waiting for the full download.

```python
from datetime import datetime, timezone
from restreamsolutions import Pad, StageNameFilters

pad = Pad(id=681)

data_obj = pad.get_data(
    start_datetime=datetime(2025, 9, 9, tzinfo=timezone.utc),
    end_datetime=datetime(2025, 9, 9, minute=1, tzinfo=timezone.utc),
    is_routed=True,
    stage_name_filter=StageNameFilters.FRAC
)

for one_second_item in data_obj.data_fetcher:
    print(one_second_item)
```

#### Saving data to a file

You can save the data to a JSON or CSV file. The format is chosen by the file extension (.json or .csv). 
If `overwrite=False` (the default) and the file already exists, a `FileExistsError` will be raised.

```python
from datetime import datetime, timezone
from restreamsolutions import Pad, StageNameFilters

pad = Pad(id=681)

data_obj = pad.get_data(
    stage_number=1,
    is_routed=True,
    stage_name_filter=StageNameFilters.FRAC
)

# Save as JSON
data_obj.save('./data/data.json', overwrite=True)

# Or save as CSV
data_obj.save('./data/data.csv', overwrite=True)
```

### Handling Data Changes

Occasionally, historical data may be corrected. You can get information about which sites and time periods were 
affected and download only the updated data.

The `get_data_changes()` method (and its async version `aget_data_changes()`) is available for `Site` and `Pad` classes.
It returns a tuple containing a list of `DataChanges` objects and a single `Data` or `DataAsync` object for fetching 
 the corresponding data.

After receiving and processing the changes, you need to confirm their retrieval. This ensures that the next time you 
check for data changes, the ones already processed will be excluded. To do this, call `confirm_data_received()`
(or `aconfirm_data_received()` for the async version) on the `DataChanges` objects you have handled.

```python
from restreamsolutions import Pad

pad = Pad(id=668)
change_events, changed_data = pad.get_data_changes()

# Inspect the change events
for event in change_events:
    print(f"ID: {event.id}, "
          f"Type: {event.modification_type}, "
          f"Start: {event.start_date}, "
          f"End: {event.end_date}")

# Fetch and process the data for all changes combined
for one_second_item in changed_data.data_fetcher:
    print(one_second_item)

# Save the changed data to a JSON file
changed_data.save('./data/data_changes.json', overwrite=True)
# Save the changed data to a CSV file
changed_data.save('./data/data_changes.csv', overwrite=True)

# Confirm that all data change events have been received and processed
for event in change_events:
    event.confirm_data_received()
```

You can also fetch the data for a single, specific change event.

```python
from restreamsolutions import Pad

pad = Pad(id=668)
change_events, _ = pad.get_data_changes()

if change_events:
    first_change_event = change_events[0]
    first_event_data = first_change_event.get_data()

    for one_second_item in first_event_data.data_fetcher:
        print(one_second_item)

    # Confirm that you have received and processed the change event
    first_change_event.confirm_data_received()
```

### Real-time data via WebSockets

The SDK supports receiving data in real time over WebSocket. The following stream types and methods are available:
- Data change events (metadata only): `get_realtime_data_changes_updates()` / `aget_realtime_data_changes_updates()`
- Site/Pad instance updates (includes site states updates): `get_realtime_instance_updates()` / `aget_realtime_instance_updates()`
- Measurement streams: `get_realtime_measurements_data()` / `aget_realtime_measurements_data()`

Common behavior and tips:
- All methods return lazy `Data`/`DataAsync` objects; read incoming messages by iterating over `data_fetcher`.
- Use `get_*` for synchronous code, and `aget_*` for asynchronous code.
- Streams can be saved to files using the `save(...)`/`asave(...)` method on the returned `Data`/`DataAsync`.
- For measurement streams, a `session_key` is also returned so that you can resume reading the queue after a process restart — details are provided in the dedicated section below.

Detailed subsections for each real-time option are provided below.

#### Real-time data change events

In addition to periodically checking for changes via `get_data_changes()`/`aget_data_changes()`, you can subscribe to a
live stream of data-change events over WebSocket using `get_realtime_data_changes_updates()` and
`aget_realtime_data_changes_updates()` on `Site` and `Pad` objects.

By default, these functions yield `DataChanges` instances (rich objects with methods like `confirm_data_received()`). 
If you prefer to receive raw dictionaries, pass `as_dict=True`.

Note: the stream contains only change metadata (IDs, time windows, types, etc.) — it does not include the changed 
data itself. To load the actual records, use the `Data`/`DataAsync` objects returned by
`get_data_changes()` / `aget_data_changes()` on `Site`/`Pad`.

```python
from restreamsolutions import Pad

pad = Pad(id=681)
updates = pad.get_realtime_data_changes_updates()  # yields DataChanges instances by default

for event in updates.data_fetcher:
    # event is a DataChanges instance
    print(event.id, event.modification_type)
    # You can immediately confirm receipt if desired
    # event.confirm_data_received()
```

#### Real-time Sites and Pads updates

You can subscribe to a continuous stream of real-time updates for a Pad (or Site) via WebSocket.
The method returns a lazy `Data`/`DataAsync` object whose `data_fetcher` yields updates one by one.
Use `get_realtime_instance_updates()` and `aget_realtime_instance_updates()` methods of the `Pad` and `Site` classes.

By default, these methods yield model instances (`Pad` or `Site`). If you prefer to receive raw dictionaries, 
pass `as_dict=True`.

```python
from restreamsolutions import Pad

pad = Pad(id=681)
updates = pad.get_realtime_instance_updates()  # yields Pad instances by default

# Iterate over incoming messages (blocking loop)
for pad_update in updates.data_fetcher:
    # item is a Pad instance
    print(pad_update.id, pad_update.name)
    # Add your own break condition if needed
    # if should_stop():
    #     break

# You can also persist streamed updates as JSON
# updates.save('./data/pad_realtime_updates.json', overwrite=True)
```

#### Real-time Measurements Data

Use the following methods to open a WebSocket stream with measurements for a Site or Pad:
- Pad: `get_realtime_measurements_data()` and `aget_realtime_measurements_data()`
- Site: `get_realtime_measurements_data()` and `aget_realtime_measurements_data()`

These methods accept the same filter parameters as `get_data()`/`aget_data()`.

Important behavior of filters:
- If you DO pass any filters (for example start_datetime/end_datetime, fields, stage filters, etc.), the stream will
first replay the historical data that matches those filters and then continue with real-time updates.
- If you DO NOT pass any filters, only fresh real-time updates will be delivered. No historical backlog will be sent.

```python
from datetime import datetime, timezone
from restreamsolutions import Pad, StageNameFilters

pad = Pad(id=681)

# With filters: first get historical, then live
stream, session_key = pad.get_realtime_measurements_data(
    start_datetime=datetime(2025, 9, 9, tzinfo=timezone.utc),
    end_datetime=datetime(2025, 9, 9, minute=1, tzinfo=timezone.utc),
    is_routed=True,
    stage_name_filter=StageNameFilters.FRAC,
    fields=["down_hole_pressure", "slurry_rate"]
)
for item in stream.data_fetcher:
    print(item)

# Without filters: live only (no historical replay)
live_only_stream, live_session_key = pad.get_realtime_measurements_data()
for item in live_only_stream.data_fetcher:
    print(item)
```

**VERY IMPORTANT: session_key usage**
- The SDK maintains resilient WebSocket connections and will automatically reuse the same session_key to continue reading from the same message queue after transient network errors or normal closes (when restart flags are enabled).
- If your whole Python process crashes or is restarted, you may want to resume from where you left off to avoid missing updates. To do so, persist the session_key returned as the second value from the method call (e.g., `data, session_key = pad.get_realtime_measurements_data(...)`) in a durable store (database, etc.), and supply it on the next start.
- **Never create multiple concurrent connections that use the same session_key**. Doing so can lead to incorrect results or duplicated messages for each connected client.

## Running tests

Run from the repository root folder

```shell
pytest
```

## License

This project is distributed under the MIT License. See the `LICENSE` file.