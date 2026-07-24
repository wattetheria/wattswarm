#!/usr/bin/env bash

set -euo pipefail

readonly backend="${WATTSWARM_STORAGE_BACKEND:-postgres}"
readonly sqlite_db_path="${WATTSWARM_SQLITE_DB_PATH:-/var/lib/wattswarm/wattswarm.db}"
readonly wait_attempts="${WATTSWARM_SQLITE_FDW_WAIT_ATTEMPTS:-120}"
readonly schema_name="wattswarm_sqlite"
readonly server_name="wattswarm_sqlite_server"
readonly -a psql_command=(psql --no-psqlrc --set=ON_ERROR_STOP=1)

if [[ ! "${wait_attempts}" =~ ^[1-9][0-9]*$ ]]; then
  echo "WATTSWARM_SQLITE_FDW_WAIT_ATTEMPTS must be a positive integer" >&2
  exit 2
fi

cleanup_mapping() {
  "${psql_command[@]}" <<SQL
BEGIN;
DROP SERVER IF EXISTS ${server_name} CASCADE;
DROP SCHEMA IF EXISTS ${schema_name} CASCADE;
COMMIT;
SQL
}

if [[ "${backend,,}" != "sqlite" ]]; then
  cleanup_mapping
  echo "SQLite live query schema disabled for backend '${backend}'"
  exit 0
fi

for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
  if [[ -r "${sqlite_db_path}" ]]; then
    break
  fi
  if ((attempt == wait_attempts)); then
    echo "SQLite database did not become readable: ${sqlite_db_path}" >&2
    exit 1
  fi
  sleep 1
done

for ((attempt = 1; attempt <= wait_attempts; attempt++)); do
  if "${psql_command[@]}" --set=sqlite_db_path="${sqlite_db_path}" <<'SQL'
BEGIN;
CREATE EXTENSION IF NOT EXISTS sqlite_fdw;
DROP SERVER IF EXISTS wattswarm_sqlite_server CASCADE;
DROP SCHEMA IF EXISTS wattswarm_sqlite CASCADE;
CREATE SCHEMA wattswarm_sqlite;
CREATE SERVER wattswarm_sqlite_server
  FOREIGN DATA WRAPPER sqlite_fdw
  OPTIONS (database :'sqlite_db_path');
IMPORT FOREIGN SCHEMA main
  FROM SERVER wattswarm_sqlite_server
  INTO wattswarm_sqlite;
REVOKE CREATE ON SCHEMA wattswarm_sqlite FROM PUBLIC;
GRANT USAGE ON SCHEMA wattswarm_sqlite TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA wattswarm_sqlite TO PUBLIC;
COMMIT;
SQL
  then
    missing_tables="$(
      "${psql_command[@]}" --tuples-only --no-align <<'SQL'
SELECT COUNT(*)
FROM (VALUES ('events'), ('executor_registry_local'), ('runs')) AS required(name)
WHERE NOT EXISTS (
  SELECT 1
  FROM information_schema.foreign_tables
  WHERE foreign_table_schema = 'wattswarm_sqlite'
    AND foreign_table_name = required.name
);
SQL
    )"
    if [[ "${missing_tables}" == "0" ]]; then
      mapped_tables="$(
        "${psql_command[@]}" --tuples-only --no-align <<'SQL'
SELECT COUNT(*)
FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'wattswarm_sqlite';
SQL
      )"
      echo "SQLite live query schema ready: ${mapped_tables} tables"
      exit 0
    fi
  fi

  if ((attempt == wait_attempts)); then
    echo "SQLite schema did not become ready: ${sqlite_db_path}" >&2
    exit 1
  fi
  sleep 1
done
