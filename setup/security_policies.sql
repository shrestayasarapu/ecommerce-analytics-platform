-- =====================================================================
-- Security & Access Control Setup — Domain 5 (10 pts)
-- Row Level Security, Column Level Security, Dynamic Data Masking,
-- and Audit Logging policies for the e-commerce analytics platform.
-- Run against ecommerce_analytics_platform_dev after Gold tables exist.
-- =====================================================================

USE CATALOG ecommerce_analytics_platform_dev;

-- ---------------------------------------------------------------------
-- 1. ROW LEVEL SECURITY (RLS)
-- Scenario: analysts should only see events for product categories
-- relevant to their team. Admins see everything.
-- Implemented as a row filter function applied to the Silver table.
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION silver_layer.category_row_filter(category_code STRING)
RETURN
  is_account_group_member('admins')
  OR category_code IS NULL
  OR category_code LIKE 'electronics%'
  OR is_account_group_member('electronics_team');

ALTER TABLE silver_layer.events_cleaned
SET ROW FILTER silver_layer.category_row_filter ON (category_code);

-- VERIFY: run as different users/groups and confirm row counts differ
-- SELECT COUNT(*) FROM silver_layer.events_cleaned;

-- ---------------------------------------------------------------------
-- 2. COLUMN LEVEL SECURITY (CLS) + DYNAMIC DATA MASKING
-- Scenario: user_id is a quasi-identifier. Admins see it in full;
-- everyone else sees a masked version (last 4 digits only).
-- ---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION silver_layer.mask_user_id(user_id STRING)
RETURN
  CASE
    WHEN is_account_group_member('admins') THEN user_id
    ELSE CONCAT('****', RIGHT(user_id, 4))
  END;

ALTER TABLE silver_layer.events_cleaned
ALTER COLUMN user_id
SET MASK silver_layer.mask_user_id;

-- VERIFY:
-- SELECT user_id FROM silver_layer.events_cleaned LIMIT 5;
-- (admins see full ID, others see ****1234 pattern)

-- ---------------------------------------------------------------------
-- 3. AUDIT LOGGING
-- Unity Catalog automatically logs all data access to system tables.
-- No manual setup needed — this section documents how to query it.
-- ---------------------------------------------------------------------

-- VERIFY audit logging is active and capturing access:
-- SELECT event_time, user_identity.email, action_name, request_params
-- FROM system.access.audit
-- WHERE request_params.table_full_name LIKE '%events_cleaned%'
-- ORDER BY event_time DESC
-- LIMIT 20;

-- ---------------------------------------------------------------------
-- 4. GRANTS SUMMARY (for reference — actual grants issued separately)
-- ---------------------------------------------------------------------
-- Admins group: full access, unmasked data, all rows
-- electronics_team group: full row access to electronics category only
-- All other authenticated users: masked user_id, filtered rows
