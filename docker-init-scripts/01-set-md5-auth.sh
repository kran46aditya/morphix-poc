#!/bin/bash
# Set PostgreSQL to use md5 authentication for Hive compatibility
set -e

# Update postgresql.conf to use md5
echo "password_encryption = md5" >> /var/lib/postgresql/data/pgdata/postgresql.conf

# Update pg_hba.conf to use md5 for all connections
sed -i 's/scram-sha-256/md5/g' /var/lib/postgresql/data/pgdata/pg_hba.conf

echo "PostgreSQL authentication set to md5"

