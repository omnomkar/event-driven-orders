-- Runs once, when the Postgres volume is first created.
-- One database and one login role per service. Each role owns its own
-- database and cannot connect to the others.

\getenv order_password ORDER_DB_PASSWORD
\getenv payment_password PAYMENT_DB_PASSWORD
\getenv inventory_password INVENTORY_DB_PASSWORD

CREATE ROLE order_svc LOGIN PASSWORD :'order_password';
CREATE ROLE payment_svc LOGIN PASSWORD :'payment_password';
CREATE ROLE inventory_svc LOGIN PASSWORD :'inventory_password';

CREATE DATABASE orders OWNER order_svc;
CREATE DATABASE payments OWNER payment_svc;
CREATE DATABASE inventory OWNER inventory_svc;

-- By default every role may connect to every database. Remove that, so only
-- the owner (and superusers) can connect.
REVOKE CONNECT ON DATABASE orders FROM PUBLIC;
REVOKE CONNECT ON DATABASE payments FROM PUBLIC;
REVOKE CONNECT ON DATABASE inventory FROM PUBLIC;
