# MongoDB Cursor Instructions for Schema Inference & Streaming

Complete guide for using MongoDB cursors (sync or async) to sample and stream documents into the schema-inference endpoint or module.

## Quick Recommendations

**For Schema Inference:**
- Use a **random sample** of documents (aggregation `$sample`) up to ~1k–10k docs depending on variety
- Random sample exposes polymorphism and field variations

**For Full Ingestion / Streaming:**
- Use a **batched cursor scan** with `batch_size` and resume by `_id` (do not use large `skip`)
- Always stream in batches; never load entire collections into memory

## Prerequisites

```bash
# For sync operations
pip install pymongo requests

# For async operations
pip install motor httpx
```

**Python Version:** 3.10+ (adjust as needed)

## Important Concepts

- **`batch_size`**: Controls memory and network round-trips. Use 500–5000 depending on document size and memory.
- **Avoid `skip()`**: On large collections, prefer `_id` range pagination or aggregation with `$sample`.
- **BSON Conversion**: Convert BSON types (ObjectId, datetime, Decimal128) to JSON-serializable forms before sending to REST/LLM.
- **Cursor Timeout**: Use `no_cursor_timeout=True` for long-running scans and close cursor explicitly.
- **Read Preference**: For production reads, consider `read_preference='secondaryPreferred'` to offload primaries.

---

## Utility: BSON → JSON-Serializable Converter

**File:** `src/core/utils/bson_convert.py`

```python
from bson import ObjectId, Decimal128
from datetime import datetime
import base64


def bson_safe(value):
    """
    Recursively convert BSON types to JSON-serializable Python types.
    
    Handles:
    - ObjectId -> str
    - datetime -> ISO string
    - Decimal128 -> str
    - bytes -> base64 string
    - Nested dicts and lists
    """
    if value is None:
        return None
    
    if isinstance(value, ObjectId):
        return str(value)
    
    if isinstance(value, datetime):
        return value.isoformat()
    
    if isinstance(value, Decimal128):
        return str(value)  # or Decimal(value.to_decimal())
    
    if isinstance(value, bytes):
        return base64.b64encode(value).decode('ascii')
    
    if isinstance(value, dict):
        return {k: bson_safe(v) for k, v in value.items()}
    
    if isinstance(value, list):
        return [bson_safe(v) for v in value]
    
    return value


# Usage: safe_doc = bson_safe(mongo_doc)
```

---

## Pattern A: Random Sampling (Recommended for Schema Inference)

Use MongoDB aggregation `$sample` to get a random sample without scanning the whole collection server-side.

**File:** `scripts/sample_via_aggregate.py`

```python
from pymongo import MongoClient
from bson.json_util import dumps, loads
import requests
import json
import os

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:pass@host:27017")
API_URL = os.getenv("API_URL", "http://localhost:8000/schema/generate")
DB = os.getenv("MONGO_DB", "mydb")
COL = os.getenv("MONGO_COLLECTION", "mycollection")
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "2000"))

# Connect to MongoDB
client = MongoClient(MONGO_URI)
coll = client[DB][COL]

# Use aggregation $sample for random sampling
print(f"Sampling {SAMPLE_SIZE} random documents from {DB}.{COL}...")
cursor = coll.aggregate([{"$sample": {"size": SAMPLE_SIZE}}])

# Convert to JSON-safe Python objects
batch = []
for doc in cursor:
    batch.append(json.loads(dumps(doc)))

print(f"Sampled {len(batch)} documents")

# POST to schema-inference endpoint
if batch:
    payload = {
        "sample_metadata": {
            "collection": COL,
            "sample_strategy": "random",
            "sample_size": len(batch)
        },
        "docs": batch
    }
    
    resp = requests.post(
        API_URL,
        json=payload,
        timeout=60,
        headers={"Content-Type": "application/json"}
    )
    
    resp.raise_for_status()
    print(f"Schema inference completed: {resp.status_code}")
    print(resp.json())
else:
    print("No documents sampled")

client.close()
```

**Notes:**
- `$sample` is efficient for moderate sizes but can be expensive for huge collections
- If collection is sharded, `$sample` behaves differently; test performance
- For very large collections, consider stratified sampling by a high-cardinality field

---

## Pattern B: Batched Cursor Scan (Full/Large Collections)

Scan by `_id` range and persist `last_processed_id` for resumability.

**File:** `scripts/stream_cursor_batches.py`

```python
from pymongo import MongoClient
from bson.json_util import dumps, loads
import requests
import json
import os
import time

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:pass@host:27017")
API_URL = os.getenv("API_URL", "http://localhost:8000/schema/ingest_batch")
DB = os.getenv("MONGO_DB", "mydb")
COL = os.getenv("MONGO_COLLECTION", "mycollection")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
RESUME_FILE = os.getenv("RESUME_FILE", "/tmp/last_processed_id.txt")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
coll = client[DB][COL]

# Load last processed ID for resumability
last_id = None
if os.path.exists(RESUME_FILE):
    with open(RESUME_FILE, 'r') as f:
        last_id_str = f.read().strip()
        if last_id_str:
            from bson import ObjectId
            last_id = ObjectId(last_id_str)
            print(f"Resuming from _id: {last_id}")

# Build query
query = {}
if last_id:
    query["_id"] = {"$gt": last_id}

# Create cursor with batch size and no timeout
cursor = coll.find(
    query,
    projection=None,
    batch_size=BATCH_SIZE,
    no_cursor_timeout=True
)

total_processed = 0
batch_count = 0

try:
    batch = []
    for doc in cursor:
        # Convert BSON to JSON-safe
        safe_doc = json.loads(dumps(doc))
        batch.append(safe_doc)
        
        if len(batch) >= BATCH_SIZE:
            # Prepare payload
            payload = {
                "sample_metadata": {
                    "collection": COL,
                    "sample_strategy": "cursor",
                    "batch_index": batch_count
                },
                "docs": batch
            }
            
            # POST batch with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    r = requests.post(
                        API_URL,
                        json=payload,
                        timeout=120,
                        headers={
                            "Content-Type": "application/json",
                            "X-Batch-Index": str(batch_count),
                            "X-Last-Id": str(batch[-1]['_id'])
                        }
                    )
                    r.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        print(f"Retry {attempt + 1}/{max_retries} after {wait_time}s: {e}")
                        time.sleep(wait_time)
                    else:
                        raise
            
            # Update last processed ID
            last_id = batch[-1]['_id']
            with open(RESUME_FILE, 'w') as f:
                f.write(str(last_id))
            
            total_processed += len(batch)
            batch_count += 1
            print(f"Processed batch {batch_count}: {len(batch)} docs (total: {total_processed})")
            
            batch = []
    
    # Process remaining batch
    if batch:
        payload = {
            "sample_metadata": {
                "collection": COL,
                "sample_strategy": "cursor",
                "batch_index": batch_count
            },
            "docs": batch
        }
        
        r = requests.post(API_URL, json=payload, timeout=120)
        r.raise_for_status()
        
        total_processed += len(batch)
        print(f"Processed final batch: {len(batch)} docs (total: {total_processed})")

finally:
    cursor.close()
    client.close()
    print(f"Streaming complete. Total documents processed: {total_processed}")
```

**Notes:**
- Store `last_id` persistently (metadata table or file) to resume after failure
- Use `dumps/loads` to make `_id` JSON-friendly
- Implement retry logic with exponential backoff for transient errors

---

## Pattern C: Async Streaming (Motor + HTTPX)

**File:** `scripts/async_stream.py`

```python
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
from bson.json_util import dumps, loads
import os
import json

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:pass@host:27017")
API_URL = os.getenv("API_URL", "http://localhost:8000/schema/ingest_batch")
DB = os.getenv("MONGO_DB", "mydb")
COL = os.getenv("MONGO_COLLECTION", "mycollection")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))


async def stream():
    """Async streaming from MongoDB to schema API."""
    client = AsyncIOMotorClient(MONGO_URI)
    coll = client[DB][COL]
    
    cursor = coll.find({}, batch_size=BATCH_SIZE)
    
    async with httpx.AsyncClient(timeout=60.0) as http:
        batch = []
        batch_index = 0
        
        async for doc in cursor:
            # Convert BSON to JSON-safe
            safe_doc = loads(dumps(doc))
            batch.append(safe_doc)
            
            if len(batch) >= BATCH_SIZE:
                payload = {
                    "sample_metadata": {
                        "collection": COL,
                        "sample_strategy": "async_cursor",
                        "batch_index": batch_index
                    },
                    "docs": batch
                }
                
                try:
                    resp = await http.post(API_URL, json=payload)
                    resp.raise_for_status()
                    print(f"Batch {batch_index} sent: {len(batch)} docs")
                    batch_index += 1
                    batch = []
                except httpx.HTTPError as e:
                    print(f"Error sending batch {batch_index}: {e}")
                    raise
        
        # Send remaining batch
        if batch:
            payload = {
                "sample_metadata": {
                    "collection": COL,
                    "sample_strategy": "async_cursor",
                    "batch_index": batch_index
                },
                "docs": batch
            }
            resp = await http.post(API_URL, json=payload)
            resp.raise_for_status()
            print(f"Final batch sent: {len(batch)} docs")
    
    client.close()
    print("Async streaming complete")


if __name__ == "__main__":
    asyncio.run(stream())
```

**Notes:**
- Motor provides async MongoDB operations
- HTTPX provides async HTTP client
- Better for high-throughput scenarios

---

## Pattern D: Incremental / CDC (Change Streams)

For live schema updates or incremental ingestion, use MongoDB Change Streams. Requires replica set.

**File:** `scripts/change_stream_cdc.py`

```python
from pymongo import MongoClient
from bson.json_util import dumps, loads
import requests
import json
import os
import time

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://user:pass@host:27017")
API_URL = os.getenv("API_URL", "http://localhost:8000/schema/ingest_batch")
DB = os.getenv("MONGO_DB", "mydb")
COL = os.getenv("MONGO_COLLECTION", "mycollection")
RESUME_TOKEN_FILE = os.getenv("RESUME_TOKEN_FILE", "/tmp/resume_token.txt")

# Connect to MongoDB (must be replica set)
client = MongoClient(MONGO_URI)
coll = client[DB][COL]

# Load resume token if available
resume_token = None
if os.path.exists(RESUME_TOKEN_FILE):
    with open(RESUME_TOKEN_FILE, 'r') as f:
        token_str = f.read().strip()
        if token_str:
            resume_token = json.loads(token_str)
            print(f"Resuming from token: {resume_token}")

# Watch collection for changes
pipeline = []  # Optional: filter specific operations
options = {"full_document": "updateLookup"}

if resume_token:
    options["resume_after"] = resume_token

with coll.watch(pipeline=pipeline, **options) as stream:
    batch = []
    batch_size = 100
    
    for change in stream:
        operation_type = change.get("operationType")
        doc = change.get("fullDocument")
        
        if doc:
            safe_doc = json.loads(dumps(doc))
            batch.append({
                "operation": operation_type,
                "document": safe_doc
            })
            
            # Save resume token periodically
            if change.get("_id"):
                with open(RESUME_TOKEN_FILE, 'w') as f:
                    json.dump(change["_id"], f)
        
        # Send batch when full
        if len(batch) >= batch_size:
            payload = {
                "sample_metadata": {
                    "collection": COL,
                    "sample_strategy": "change_stream",
                    "operations": [b["operation"] for b in batch]
                },
                "docs": [b["document"] for b in batch]
            }
            
            try:
                r = requests.post(API_URL, json=payload, timeout=60)
                r.raise_for_status()
                print(f"Sent batch of {len(batch)} changes")
                batch = []
            except requests.exceptions.RequestException as e:
                print(f"Error sending batch: {e}")
                # Continue processing; batch will be retried

client.close()
```

**Notes:**
- Change Streams are ideal for streaming CDC into Hudi upserts
- Handle resume tokens and persist them to resume after outages
- Requires replica set configuration

---

## Posting Strategy & API Contract

### Single Batch (Schema Inference)

If your schema-inference endpoint accepts `sample_docs`, send a single batch up to ~5k docs:

```python
payload = {
    "sample_metadata": {
        "collection": "mycollection",
        "sample_strategy": "random",
        "sample_size": 2000
    },
    "docs": [{"_id": "...", "userId": "u1", ...}]
}
```

### Streaming Batches (Full Ingestion)

If it supports streaming batches, POST `{"docs": [...]}` repeatedly:

```python
headers = {
    "Content-Type": "application/json",
    "X-Batch-Index": str(batch_index),
    "X-Last-Id": str(last_doc_id),
    "X-Idempotency-Key": f"{collection}_{batch_index}"  # For idempotency
}
```

### Sample Metadata Object

Always include `sample_metadata`:

```python
{
    "sample_metadata": {
        "collection": "mycollection",
        "sample_strategy": "random" | "cursor" | "change_stream",
        "sample_size": 2000,
        "batch_index": 0,  # For streaming
        "timestamp": "2024-01-01T00:00:00Z"
    },
    "docs": [...]
}
```

---

## Error Handling & Retries

```python
import time
from requests.exceptions import RequestException

def send_with_retry(url, payload, max_retries=3, backoff_factor=2):
    """Send request with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, timeout=120)
            resp.raise_for_status()
            return resp
        except RequestException as e:
            if attempt < max_retries - 1:
                wait_time = backoff_factor ** attempt
                print(f"Retry {attempt + 1}/{max_retries} after {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                raise
```

**Guidelines:**
- Use idempotency keys (e.g., `X-Idempotency-Key`) for batch handlers
- Retry transient network errors (5xx) with exponential backoff
- For 4xx errors, surface the error (don't retry)
- Validate incoming batch size and reject unusually large batches early

---

## Heuristics for Sample Sizes

| Collection Size | Sample Size | Strategy |
|----------------|-------------|----------|
| Small/simple (<10K docs) | 200–500 docs | Random sample |
| Medium (10K–1M docs) | 1k–5k docs | Random sample |
| High-cardinality/polymorphic | 1k–10k docs | Random sample |
| Very large (>100M docs) | 5k–10k docs | Stratified sampling or per-shard sampling |

**Stratified Sampling Example:**

```python
# Sample by userId modulo N for stratified sampling
pipeline = [
    {"$match": {"userId": {"$exists": True}}},
    {"$addFields": {"hash": {"$mod": [{"$toInt": {"$substr": ["$userId", -1, 1]}}, 10]}}},
    {"$match": {"hash": 0}},  # Sample 1/10th
    {"$sample": {"size": 1000}}
]
```

---

## Security & PII

### Mask Sensitive Fields

```python
PII_FIELDS = ["ssn", "credit_card", "password", "email", "phone"]

def redact_pii(doc):
    """Remove or mask PII fields before sending to API."""
    safe_doc = doc.copy()
    for field in PII_FIELDS:
        if field in safe_doc:
            safe_doc[field] = "[REDACTED]"
    return safe_doc

# Usage
safe_doc = redact_pii(bson_safe(mongo_doc))
```

### Secure Transport

- Use HTTPS for API communication
- Use JWT tokens for authentication
- Consider mutual TLS for production
- Never log sensitive data

---

## CLI Pattern

**File:** `scripts/stream_to_schema.py`

```python
#!/usr/bin/env python3
"""CLI tool for streaming MongoDB documents to schema API."""

import argparse
import sys
from scripts.sample_via_aggregate import sample_random
from scripts.stream_cursor_batches import stream_batches

def main():
    parser = argparse.ArgumentParser(description="Stream MongoDB to Schema API")
    parser.add_argument("--mongo-uri", required=True, help="MongoDB connection URI")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--collection", required=True, help="Collection name")
    parser.add_argument("--api-url", required=True, help="Schema API URL")
    parser.add_argument("--strategy", choices=["random", "cursor", "async"], default="random")
    parser.add_argument("--sample-size", type=int, default=2000, help="Sample size for random")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for cursor")
    parser.add_argument("--resume", help="Resume file path")
    
    args = parser.parse_args()
    
    if args.strategy == "random":
        sample_random(
            mongo_uri=args.mongo_uri,
            db=args.db,
            collection=args.collection,
            api_url=args.api_url,
            sample_size=args.sample_size
        )
    elif args.strategy == "cursor":
        stream_batches(
            mongo_uri=args.mongo_uri,
            db=args.db,
            collection=args.collection,
            api_url=args.api_url,
            batch_size=args.batch_size,
            resume_file=args.resume
        )
    else:
        print("Async strategy not implemented in CLI yet")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

**Usage:**
```bash
python scripts/stream_to_schema.py \
    --mongo-uri "mongodb://localhost:27017" \
    --db "mydb" \
    --collection "mycollection" \
    --api-url "http://localhost:8000/schema/generate" \
    --strategy "random" \
    --sample-size 2000
```

---

## Troubleshooting

### Cursor Dies with CursorNotFound

**Solution:**
- Commit batches frequently
- Increase server `cursorTimeoutMillis`
- Use `no_cursor_timeout=True` carefully (close cursor explicitly)
- For long scans, use `_id` range pagination instead of single cursor

### Aggregation $sample Slow

**Solutions:**
- Try reservoir sampling client-side
- Use hashed bucketing on a stable field
- For sharded collections, sample per-shard and combine

### Large BSON Types Fail JSON Encode

**Solution:**
- Use `bson.json_util.dumps/loads`
- Or use the `bson_safe` utility provided above
- Handle Decimal128, Binary, and other complex types explicitly

### Memory Issues with Large Batches

**Solution:**
- Reduce `batch_size` (try 500 instead of 5000)
- Process and send batches immediately
- Use async streaming for better memory management

---

## File Structure

```
morphix-poc/
├── src/
│   ├── core/
│   │   ├── utils/
│   │   │   └── bson_convert.py      # BSON conversion utility
│   │   └── schema_inference.py       # Inference logic
│   └── ...
├── scripts/
│   ├── sample_via_aggregate.py      # Random sampling
│   ├── stream_cursor_batches.py      # Batch streaming
│   ├── async_stream.py               # Async streaming
│   ├── change_stream_cdc.py          # Change streams
│   └── stream_to_schema.py           # CLI tool
└── docs/
    └── CURSOR_INSTRUCTIONS.md        # This file
```

---

## Minimal Checklist to Run

1. **Install dependencies:**
   ```bash
   pip install pymongo requests
   # or for async
   pip install motor httpx
   ```

2. **Set environment variables:**
   ```bash
   export MONGO_URI="mongodb://user:pass@host:27017"
   export API_URL="http://localhost:8000/schema/generate"
   export MONGO_DB="mydb"
   export MONGO_COLLECTION="mycollection"
   ```

3. **Run sampling script:**
   ```bash
   python scripts/sample_via_aggregate.py
   ```

4. **Verify schema API received sample:**
   - Check API logs
   - Verify response contains schema suggestions
   - Test with different sample sizes

---

## Integration with Morphix Schema API

The Morphix schema inference endpoint expects:

```python
POST /schema/generate
{
    "sample_metadata": {
        "collection": "mycollection",
        "sample_strategy": "random",
        "sample_size": 2000
    },
    "docs": [
        {"_id": "...", "field1": "value1", ...},
        ...
    ]
}
```

Response:
```python
{
    "schema": {
        "field1": {"type": "string", "nullable": false},
        ...
    },
    "suggestions": [...],
    "quality_flags": [...],
    "complexity_score": 45.2
}
```

---

## Best Practices Summary

1. ✅ **Always use batches** - Never load entire collections into memory
2. ✅ **Use random sampling for schema inference** - Exposes polymorphism
3. ✅ **Use cursor pagination for full ingestion** - Avoid `skip()` on large collections
4. ✅ **Convert BSON to JSON** - Use provided utilities
5. ✅ **Implement retry logic** - Handle transient errors
6. ✅ **Persist resume tokens** - Enable crash recovery
7. ✅ **Monitor batch sizes** - Adjust based on document size
8. ✅ **Use async for high throughput** - Motor + HTTPX
9. ✅ **Secure sensitive data** - Redact PII before sending
10. ✅ **Close cursors explicitly** - Prevent resource leaks

---

## Additional Resources

- [MongoDB Cursor Documentation](https://docs.mongodb.com/manual/reference/method/cursor/)
- [MongoDB Aggregation $sample](https://docs.mongodb.com/manual/reference/operator/aggregation/sample/)
- [MongoDB Change Streams](https://docs.mongodb.com/manual/changeStreams/)
- [Motor Async Driver](https://motor.readthedocs.io/)
- [Morphix Component Documentation](COMPONENTS.md)

