-- Runs once, when the PostgreSQL volume is first created.
-- poc_owner owns every table and cannot log in. app_rw is the only login the API uses:
-- it does not own the tables and cannot bypass row-level security.
-- \getenv reads the password from the container environment (psql 15 or later).
\getenv app_pw APP_DB_PASSWORD
\getenv worker_pw WORKER_DB_PASSWORD
CREATE ROLE poc_owner NOLOGIN;
CREATE ROLE app_rw LOGIN PASSWORD :'app_pw' NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
-- worker_rw is the service login for the outbox relay and the projections.
CREATE ROLE worker_rw LOGIN PASSWORD :'worker_pw' NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
CREATE SCHEMA poc AUTHORIZATION poc_owner;
ALTER ROLE app_rw SET search_path = poc, public;
ALTER ROLE worker_rw SET search_path = poc, public;
CREATE EXTENSION IF NOT EXISTS vector;
