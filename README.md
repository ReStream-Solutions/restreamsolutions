# Datastore SDK

This project provides a convenient Python SDK for interacting with the Tally Resteam API. Using this SDK, you can:
- check the configuration of your Sites and Pads and receive updates in real time
- get information about the current and historical stages of the Sites with additional aggregated metrics
- retrieve raw or sampled data from your Sites and Pads
- get a list of changes that were applied to the data, download this data, and confirm that these changes were received
- connect to a WebSocket room associated with a specific Site or Pad and receive new data in real time
  (will be implemented in the next versions)

## Installation

Via pip (from the published package):

```bash
pip install restream-datastore-sdk
```

Or from source (via Poetry):

```bash
poetry install
```

## Setup

Set the environment variable TALLY_AUTH_TOKEN and assign it your authorization token obtained from Tally.
Alternatively, you can skip setting the environment variable and pass the authorization token directly to the 
SDK classes and methods.

```python
import os
from datastore_sdk import Pad
os.environ["TALLY_AUTH_TOKEN"] = "your token"
pads = Pad.get_models()
# Or provide auth token directly to the method
pads = Pad.get_models(auth_token="your token")
```

## Usage

You will primarily interact with two classes, `Site` and `Pad`, which provide access to all the information available
through the Restream API. Please set your authorization token at the very top of your code file, as shown above. 
In the examples, this step is omitted for the sake of brevity.

### Fetching Sites and Pads

#### Get a list of all models

You can retrieve a list of all `Pads` or `Sites` available to you.

By default, the SDK returns a list of class objects (Pad or Site). During serialization, all fields from the HTTP
response become attributes of the class. Fields containing timestamps are automatically converted to timezone-aware
Python `datetime` objects.

```python
from datastore_sdk import Pad, Site
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
from datastore_sdk import Pad, Site
pads_as_dict = Pad.get_models(as_dict=True)
for pad in pads_as_dict:
    print(f'pad id: {pad["id"]}, pad name: {pad["name"]}, simops config: {pad["simops_config"]}')

# Similarly, for sites
sites_as_dict = Site.get_models(as_dict=True)
```

#### Get a single model by ID

If you know the ID of a `Site` or `Pad`, you can fetch a specific object using the `get_model` class method.

```python
from datastore_sdk import Pad, Site
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
from datastore_sdk import Site
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
from datastore_sdk import Pad
# This approach makes two API calls: one to get the Pad, another to get its Sites.
sites_v1 = Pad.get_model(id=668).get_sites()

# This is more efficient, making only one API call to get the Sites.
sites_v2 = Pad(id=668).get_sites()
``` 

When you create an object with only an `id`, it won't have other attributes populated. You can still use its instance 
methods to fetch related data. To load the object's own attributes, use the `update()` or `aupdate()` (async) method.

```python
from datastore_sdk import Pad
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
from datastore_sdk import Site
pad = Site(id=981).get_pad()
pad_as_dict = Site(id=981).get_pad(as_dict=True)
```

### Getting and Monitoring Site State

#### Getting the current state

To get the current configuration of a specific site or all sites on a pad, use the `get_state()` 
and `get_states()` methods.

```python
from datastore_sdk import Site, Pad
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
from datastore_sdk import Pad, StageNameFilters
pad = Pad(id=668)
frac_states = pad.get_states(stage_name_filter=StageNameFilters.FRAC)
```

#### Monitoring state for real-time updates

Since the current State of a site changes, you can monitor it by calling the `update()` method on the state object.

```python
import time
from datastore_sdk import Site, StageNameFilters
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
from datastore_sdk import Site, StageNameFilters
site = Site(id=981)
start_date = datetime(2025, 9, 17, 0, 0, 0, tzinfo=timezone.utc)
end_date = datetime(2025, 9, 18, 0, 0, 0, tzinfo=timezone.utc)

# Get stages within a date range
stages_from_range = site.get_stages_metadata(start_date=start_date, end_date=end_date)
for stage in stages_from_range[:3]:
    print(f"ID: {stage['id']}, State: {stage['state']}, Start: {stage['start']}")

# Get stages with a specific stage number and stage name
stages_by_number = site.get_stages_metadata(stage_number=1, stage_name_filter=StageNameFilters.WIRELINE)
```

You can also include aggregated metrics for each stage by setting `add_aggregations=True`.

```python
from datetime import datetime, timezone
from datastore_sdk import Site, StageNameFilters
site = Site(id=981)
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
from datastore_sdk import Pad, Site
# For an entire pad
measurement_sources = Pad(id=681).get_measurement_sources_metadata()
print(f'Pad measurement sources: {measurement_sources}')

# For a specific site
site = Site(id=981)
measurement_sources = site.get_measurement_sources_metadata()
print(f'Site measurement sources: {measurement_sources}')
```

#### Fields Metadata

Get a list of available data field names for a pad or site. These names can be used to filter data retrieval.

```python
from datastore_sdk import Pad, Site
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

For pads, the `is_routed` parameter (default `False`) controls whether the data is distributed by its specific 
site (`True`) or returned for the entire pad (`False`). See `get_data()` documentation to get more details.

```python
from datetime import datetime, timezone
from datastore_sdk import Pad, StageNameFilters
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
from datastore_sdk import Pad, StageNameFilters
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
from datastore_sdk import Pad, StageNameFilters
pad = Pad(id=681)

data_obj = pad.get_data(
    start_datetime=datetime(2025, 9, 9, tzinfo=timezone.utc),
    end_datetime=datetime(2025, 9, 9, minute=1, tzinfo=timezone.utc),
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
It returns a tuple containing a list of `DataChange` objects and a single `Data` or `DataAsync` object for fetching 
the corresponding data.

After receiving and processing the changes, you need to confirm their retrieval. This ensures that the next time you 
check for data changes, the ones already processed will be excluded. To do this, call `confirm_data_received()`
(or `aconfirm_data_received()` for the async version) on the `DataChange` objects you have handled.

```python
from datastore_sdk import Pad
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
from datastore_sdk import Pad
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

## Running tests

Run from the repository root folder

```shell
pytest
```

## License

This project is distributed under the MIT License. See the `LICENSE` file.