# Hive Metastore Service Not Starting

## Issue
The `bde2020/hive-metastore-postgresql` container is only running PostgreSQL, not the Hive Metastore Thrift service (port 9083). This prevents Trino from connecting to query Hudi tables.

## Current Status
- ✅ Trino API endpoints are now registered and working
- ✅ Trino server is running on port 8083
- ✅ Trino client connects successfully
- ❌ Hive Metastore service is not running (only PostgreSQL is running)

## Solutions

### Option 1: Use Standalone Hive Metastore Image
Replace the current image with one that properly runs the metastore service:

```yaml
hive-metastore:
  image: apache/hive:3.1.3
  command: /opt/hive/bin/hive --service metastore
  # ... rest of config
```

**Note:** This image had timeout issues during pull. You may need to:
- Pull it separately: `docker pull apache/hive:3.1.3`
- Or configure a Docker registry mirror

### Option 2: Manually Start Metastore in Current Container
If the `bde2020/hive-metastore-postgresql` image should support it, check:

```bash
docker exec morphix-hive-metastore find / -name "*metastore*" -type f
docker exec morphix-hive-metastore find / -name "start-metastore.sh" -o -name "run-metastore.sh"
```

### Option 3: Use Separate Containers
Run Hive Metastore in a separate container that properly starts the Java service.

## Testing
Once Hive Metastore is running:

1. Verify it's listening:
   ```bash
   docker exec morphix-trino nc -zv hive-metastore 9083
   ```

2. Test Trino connection:
   ```bash
   curl -H "Authorization: Bearer TOKEN" \
     "http://localhost:8000/trino/tables?catalog=hive&schema=default"
   ```

3. Check Trino logs:
   ```bash
   docker logs morphix-trino | grep -i metastore
   ```

## Current Workaround
The end-to-end test script will gracefully handle Trino connection failures and show what succeeded. Steps 1-7 (up to data transformation) are working correctly.

