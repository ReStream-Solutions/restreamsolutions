# Datastore SDK

This project provides a convenient Python SDK for interacting with the Tally Resteam API.

## Installation

Via pip (from the published package):

```bash
pip install datastore-sdk
```

Or from source (via Poetry):

```bash
poetry install
```

## Quick Start

Minimal usage example:

```python
import os
from datastore_sdk import Pad

os.environ["TALLY_AUTH_TOKEN"] = "your token"

Pad.get_models(as_json=True)
```

- The `TALLY_AUTH_TOKEN` environment variable must contain your Tally access token.
- The `Pad.get_models` method returns the list of available models; passing `as_json=True` returns JSON-serializable structures.

## Examples

Additional scenarios can be found in the `examples/` directory.

## License

This project is distributed under the MIT License. See the `LICENSE` file.