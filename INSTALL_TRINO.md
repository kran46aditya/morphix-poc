# Installing Trino Python Client

The Trino API endpoints require the `trino` Python package to be installed.

## Installation

If you're using a virtual environment:

```bash
# Activate your virtual environment first
source venv/bin/activate  # or your venv path

# Install trino package
pip install trino==0.336.0
```

If you're using the system Python (and have PEP 668 restrictions):

```bash
# Install with user flag
pip3 install --user trino==0.336.0

# OR use a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate
pip install trino==0.336.0
```

## Verify Installation

After installation, restart your API server and verify:

```bash
python3 -c "import trino; print('Trino version:', trino.__version__)"
```

## Restart API Server

After installing the package, restart your API server:

```bash
# If using uvicorn directly
# Kill the existing process and restart

# Or if using a process manager
# Restart the service
```

The Trino router should now be registered and endpoints like `/trino/tables`, `/trino/query`, etc. should be available.

## Check Router Registration

You can verify the router is registered by checking the API docs:

```bash
curl http://localhost:8000/docs
```

Look for `/trino` endpoints in the API documentation.

