-- One-time Snowflake setup for the youtuber_analysis pipeline.
--
-- Run as a user holding ACCOUNTADMIN (the account owner on a trial), e.g.
-- paste into a Snowsight worksheet and "Run All". Every statement is
-- idempotent, so re-running is safe.
--
-- Before running, replace <PASTE_PUBLIC_KEY_HERE> with the body of
-- ~/.snowflake/keys/youtuber_analysis_svc.pub (no BEGIN/END lines).
--
-- Creates:
--   warehouse  YOUTUBER_ANALYSIS_WH       XSMALL, suspends after 60s idle
--   monitor    YOUTUBER_ANALYSIS_MONITOR  monthly credit cap on that warehouse
--   database   YOUTUBER_ANALYSIS          schemas RAW, STAGING, MARTS
--   role       YOUTUBER_ANALYSIS_ROLE     used by both Dagster (RAW) and dbt
--   user       YOUTUBER_ANALYSIS_SVC      service user, key-pair auth only

-- Capture the human running this so they can use the pipeline role in Snowsight.
SET admin_user = CURRENT_USER();

-------------------------------------------------------------------------------
-- Compute + cost guardrail
-------------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS YOUTUBER_ANALYSIS_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'youtuber_analysis: Dagster ingestion + dbt builds';

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS YOUTUBER_ANALYSIS_MONITOR
  WITH CREDIT_QUOTA = 10
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE YOUTUBER_ANALYSIS_WH SET RESOURCE_MONITOR = YOUTUBER_ANALYSIS_MONITOR;

-------------------------------------------------------------------------------
-- Storage: RAW (Dagster writes) -> STAGING (dbt views) -> MARTS (dbt tables)
-------------------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS YOUTUBER_ANALYSIS;
CREATE SCHEMA IF NOT EXISTS YOUTUBER_ANALYSIS.RAW
  COMMENT = 'Append-only API snapshots landed by Dagster';
CREATE SCHEMA IF NOT EXISTS YOUTUBER_ANALYSIS.STAGING
  COMMENT = 'dbt staging views: thin rename/cast over RAW';
CREATE SCHEMA IF NOT EXISTS YOUTUBER_ANALYSIS.MARTS
  COMMENT = 'dbt mart tables';

-------------------------------------------------------------------------------
-- Access: one role, one service user
-------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS YOUTUBER_ANALYSIS_ROLE
  COMMENT = 'Pipeline role for Dagster ingestion and dbt transformations';

-- Standard hierarchy: SYSADMIN inherits custom roles, so admins can see
-- everything the pipeline creates.
GRANT ROLE YOUTUBER_ANALYSIS_ROLE TO ROLE SYSADMIN;
GRANT ROLE YOUTUBER_ANALYSIS_ROLE TO USER IDENTIFIER($admin_user);

GRANT USAGE, OPERATE ON WAREHOUSE YOUTUBER_ANALYSIS_WH TO ROLE YOUTUBER_ANALYSIS_ROLE;

GRANT USAGE, CREATE SCHEMA ON DATABASE YOUTUBER_ANALYSIS TO ROLE YOUTUBER_ANALYSIS_ROLE;
-- CREATE STAGE / FILE FORMAT: write_pandas (used by the loaders and the
-- Snowflake pandas I/O manager) bulk-loads through a temporary stage and
-- file format, and temporary objects still need the create privilege.
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON SCHEMA YOUTUBER_ANALYSIS.RAW TO ROLE YOUTUBER_ANALYSIS_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA YOUTUBER_ANALYSIS.STAGING TO ROLE YOUTUBER_ANALYSIS_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
  ON SCHEMA YOUTUBER_ANALYSIS.MARTS TO ROLE YOUTUBER_ANALYSIS_ROLE;

-- Service users can't have passwords; key-pair is the only login method.
CREATE USER IF NOT EXISTS YOUTUBER_ANALYSIS_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = YOUTUBER_ANALYSIS_ROLE
  DEFAULT_WAREHOUSE = YOUTUBER_ANALYSIS_WH
  DEFAULT_NAMESPACE = YOUTUBER_ANALYSIS.RAW
  COMMENT = 'youtuber_analysis pipeline (Dagster + dbt)';

-- Separate ALTER so a re-run with a new key rotates it on an existing user.
ALTER USER YOUTUBER_ANALYSIS_SVC SET RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_HERE>';

GRANT ROLE YOUTUBER_ANALYSIS_ROLE TO USER YOUTUBER_ANALYSIS_SVC;

-------------------------------------------------------------------------------
-- Print the account identifier to put in .env as SNOWFLAKE_ACCOUNT
-------------------------------------------------------------------------------
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS snowflake_account;
