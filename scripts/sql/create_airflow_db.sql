-- Creates the Airflow metadata database on first PostgreSQL container start.
-- This file is mounted into /docker-entrypoint-initdb.d/ in docker-compose.yml
-- and is only executed when the postgres_data volume is empty (first init).
--
-- If the volume already exists, run this manually once:
--   docker exec de_postgres psql -U de_user -c "CREATE DATABASE airflow_db;"

SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow_db'
)\gexec
