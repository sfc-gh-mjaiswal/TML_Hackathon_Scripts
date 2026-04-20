/***********************************************************************
  ❄️ SNOWFLAKE PLATFORM OVERVIEW — TATA MOTORS LIMITED
  ---------------------------------------------------------------
  Presenter : Manisha Jaiswal
  Audience  : Tata Motors (Technical + Business Stakeholders)
  Duration  : 15–20 minutes
  Interface : Snowsight UI only
  
  ARCHITECTURE LAYERS COVERED:
  ┌─────────────────────────────────────────────────┐
  │  [1]  SETUP                                     │
  │  [2]  ELASTIC COMPUTE (CPU, GPU, Containers)    │
  │  [3]  INTEROPERABLE STORAGE (All 3 data types)  │
  │  [4]  MANAGED METADATA & OPTIMIZATION           │
  │  [5]  LANGUAGES (SQL, Python, Java, Scala)      │
  │  [6]  HORIZON (Governance, Tags, Masking)        │
  │  [7]  SECURITY & COMPLIANCE                     │
  │  [8]  CORTEX AI (LLMs, ML, Agents, Chat APIs)   │
  │  [9]  MODELS (Model Registry & Fine-tuning)     │
  │  [10] STUDIO (Marketplace & Data Sharing)       │
  │  [11] ADMINISTRATION & COST MANAGEMENT          │
  │  [12] SNOWGRID (Cross-Region & Cross-Cloud)     │
  │  [13] CLOSING                                   │
  │  [14] CLEANUP                                   │
  └─────────────────────────────────────────────────┘
  
  HOW TO USE:
  • Load in a Snowsight Worksheet
  • Run section by section (Ctrl/Cmd + Enter on highlighted block)
  • Comments starting with 🗣 are your TALK TRACK (read aloud)
  • Comments starting with 👉 are UI NAVIGATION actions (click in UI)
  • Comments starting with 💡 are KEY MESSAGES to emphasize
  
  PRE-REQUISITES:
  • Role: SYSADMIN or ACCOUNTADMIN
  • Cortex AI enabled (cross-region inference if needed)
  • Cortex Code enabled
  • Marketplace access enabled
***********************************************************************/


-- =============================================================
-- [1] SETUP — Run once before the demo (not part of live walk)
-- =============================================================

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS TML_PLATFORM_DEMO;
CREATE SCHEMA IF NOT EXISTS TML_PLATFORM_DEMO.CONNECTED_VEHICLE;
USE DATABASE TML_PLATFORM_DEMO;
USE SCHEMA CONNECTED_VEHICLE;


-- =============================================================
-- [2] ELASTIC COMPUTE — CPU, GPU, CONTAINERS (Minute 0–3)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Let's start at the heart of Snowflake — compute. Snowflake 
--  completely separates storage from compute. This means you can 
--  spin up isolated compute clusters — we call them warehouses — 
--  for each team or workload. No resource contention, ever."

-- 👉 [UI] Navigate to: Manage → Compute → Warehouses
-- 👉 [UI] Show existing warehouses, point out sizes (XS to 6XL)

-- 2a. CPU: Virtual Warehouse — instant spin-up
CREATE OR REPLACE WAREHOUSE TML_ANALYTICS_WH 
  WAREHOUSE_SIZE   = 'XSMALL'
  AUTO_SUSPEND     = 60        -- suspends after 60s idle = $0
  AUTO_RESUME      = TRUE      -- wakes instantly on next query
  COMMENT          = 'TML analytics warehouse — auto-suspends to save costs';

USE WAREHOUSE TML_ANALYTICS_WH;

-- 🗣 TALK TRACK:
-- "Created in seconds. Auto-suspends when idle, auto-resumes 
--  on demand. You only pay when it's running."

-- 2b. Scale UP (more power) — instant resize, no downtime
-- ALTER WAREHOUSE TML_ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- 2c. Scale OUT (more concurrency) — multi-cluster
ALTER WAREHOUSE TML_ANALYTICS_WH SET 
  MIN_CLUSTER_COUNT = 1 
  MAX_CLUSTER_COUNT = 3 
  SCALING_POLICY    = 'STANDARD';

-- 🗣 TALK TRACK:
-- "Scale UP doubles compute power. Scale OUT adds clusters for 
--  concurrency — hundreds of users, zero queuing. 
--  All automatic, all elastic."

-- 💡 KEY MESSAGE: "No capacity planning, no over-provisioning."

-- 2d. GPU & Containers — Snowpark Container Services (show concept)
-- 🗣 TALK TRACK:
-- "Beyond SQL warehouses, Snowflake also offers GPU compute pools 
--  for AI/ML training and container services for custom apps — 
--  all managed, all within the same security perimeter."

CREATE COMPUTE POOL TML_GPU_POOL 
MIN_NODES = 1 MAX_NODES = 3 
  INSTANCE_FAMILY = GPU_NV_S;

-- 👉 [UI] Navigate to: Manage → Compute → Compute Pools (show the page)


-- =============================================================
-- [3] INTEROPERABLE STORAGE — ALL 3 DATA TYPES (Minute 3–5)
-- =============================================================
-- 🗣 TALK TRACK:
-- "The storage layer handles ALL data types natively — structured, 
--  semi-structured, and unstructured. No separate tools, no ETL 
--  pipelines for JSON, Parquet, or even images and PDFs."

-- 3a. STRUCTURED + SEMI-STRUCTURED in one table
CREATE OR REPLACE TABLE tml_vehicle_telemetry (
  vehicle_id   VARCHAR,
  event_time   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  plant_region VARCHAR,
  payload      VARIANT         -- stores raw JSON natively
);

-- Insert JSON data directly — no pre-processing, no flattening
INSERT INTO tml_vehicle_telemetry (vehicle_id, plant_region, payload)
SELECT 'NEXON-EV-1001', 'Pune-Chikhali', PARSE_JSON('{
    "speed_kph": 72, "lat": 18.5204, "lon": 73.8567,
    "battery_pct": 65, "motor_rpm": 2400, "event": "MOVING",
    "model": "Nexon EV Max"
  }')
UNION ALL
SELECT 'NEXON-EV-1001', 'Pune-Chikhali', PARSE_JSON('{
    "speed_kph": 0, "lat": 18.5314, "lon": 73.8446,
    "battery_pct": 63, "motor_rpm": 800, "event": "IDLE",
    "model": "Nexon EV Max"
  }')
UNION ALL
SELECT 'HARRIER-2001', 'Ranjangaon', PARSE_JSON('{
    "speed_kph": 95, "lat": 19.0760, "lon": 72.8777,
    "fuel_pct": 42, "engine_rpm": 3800, "event": "OVERSPEED",
    "model": "Harrier Adventure"
  }')
UNION ALL
SELECT 'SAFARI-3001', 'Lucknow-Pantnagar', PARSE_JSON('{
    "speed_kph": 45, "lat": 28.6139, "lon": 77.2090,
    "fuel_pct": 88, "engine_rpm": 1900, "event": "MOVING",
    "model": "Safari Adventure Plus"
  }');

-- Query nested JSON with simple dot notation
SELECT 
    vehicle_id,
    event_time,
    plant_region,
    payload:speed_kph::INT        AS speed_kph,
    payload:lat::FLOAT            AS latitude,
    payload:lon::FLOAT            AS longitude,
    payload:model::STRING         AS vehicle_model,
    payload:engine_rpm::INT       AS engine_rpm,
    payload:event::STRING         AS event_type
FROM tml_vehicle_telemetry
ORDER BY event_time DESC;

-- 🗣 TALK TRACK:
-- "Dot notation on JSON — no parsing libraries, no schema-on-read 
--  complexity. This is huge for IoT and telemetry where schemas 
--  evolve constantly. No pipeline breakage, no re-modeling."

-- 3b. UNSTRUCTURED DATA — files, images, PDFs (show concept)
-- 🗣 TALK TRACK:
-- "For unstructured data — images, PDFs, videos — Snowflake stores 
--  them in stages and you can query file metadata, process with AI, 
--  or build pipelines around them."

-- List files in a stage (concept — uncomment if you have a stage)
 SELECT * FROM DIRECTORY(@tml_dashcam_stage);

-- 3c. ICEBERG TABLES — openness & interoperability (show concept)
-- 🗣 TALK TRACK:
-- "And with Iceberg Tables, your data stays in open Apache Parquet 
--  format. No vendor lock-in. Other engines can read the same data."

 CREATE ICEBERG TABLE tml_parts_catalog (part_id INT, part_name VARCHAR)
   CATALOG = 'SNOWFLAKE'
   EXTERNAL_VOLUME = 'tml_ext_vol'
  BASE_LOCATION = 'iceberg/tml_parts';

-- 💡 KEY MESSAGE: "One platform — structured, semi-structured, 
--  unstructured. Open formats. No data silos."


-- =============================================================
-- [4] MANAGED METADATA & OPTIMIZATION (Minute 5–6)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Snowflake auto-manages all metadata — statistics, micro-partition 
--  maps, result caching. Zero tuning required. Let me show you."

-- 4a. Metadata-only query — instant, minimal compute
SELECT COUNT(*) FROM tml_vehicle_telemetry;

-- 🗣 TALK TRACK:
-- "That COUNT was instant — Snowflake answered it from metadata 
--  alone, no table scan needed."

-- 4b. Result caching — run the same query twice
SELECT vehicle_id, payload:speed_kph::INT AS speed 
FROM tml_vehicle_telemetry 
WHERE payload:event::STRING = 'MOVING';

-- Run it again — instant from cache (show 0ms in query history)
SELECT vehicle_id, payload:speed_kph::INT AS speed 
FROM tml_vehicle_telemetry 
WHERE payload:event::STRING = 'MOVING';

-- 4c. Clustering — for large-scale performance
-- 🗣 TALK TRACK:
-- "For large tables — billions of rows — you define clustering keys. 
--  Snowflake automatically organizes data for partition pruning."

ALTER TABLE tml_vehicle_telemetry CLUSTER BY (vehicle_id, event_time);

-- Check clustering efficiency
-- SELECT SYSTEM$CLUSTERING_INFORMATION(
--   'tml_vehicle_telemetry', '(vehicle_id, event_time)');

-- 4d. Query performance visibility
SELECT 
    QUERY_ID,
    QUERY_TYPE,
    TOTAL_ELAPSED_TIME/1000    AS elapsed_seconds,
    BYTES_SCANNED/1024/1024    AS mb_scanned,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 5))
ORDER BY START_TIME DESC;

-- 👉 [UI] Click any query → Query Profile → show execution plan
-- 👉 [UI] Point out: partitions scanned vs total (pruning proof)

-- 💡 KEY MESSAGE: "Zero tuning. Auto-statistics, auto-caching, 
--  auto-clustering. Your team focuses on queries, not performance."


-- =============================================================
-- [5] LANGUAGES — SQL, Python, Java, Scala (Minute 6–7)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Snowflake isn't SQL-only. Your team can write in SQL, Python, 
--  Java, or Scala — all running natively inside the platform."

-- 5a. SQL — you've already seen it in action

-- 5b. Python UDF — runs natively in Snowflake
CREATE OR REPLACE FUNCTION kmh_to_mph(speed_kmh FLOAT)
  RETURNS FLOAT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  HANDLER = 'convert'
AS $$
def convert(speed_kmh):
    return round(speed_kmh * 0.621371, 1)
$$;

SELECT 
    vehicle_id,
    payload:speed_kph::FLOAT AS speed_kmh,
    kmh_to_mph(payload:speed_kph::FLOAT) AS speed_mph
FROM tml_vehicle_telemetry;

-- 🗣 TALK TRACK:
-- "A Python function running inside Snowflake — no external servers, 
--  no data movement. Same governance. And it works in Snowpark 
--  DataFrames, Notebooks, and Streamlit apps too."

-- 👉 [UI] Optionally show: Projects → Notebooks 
--         (show SQL + Python cells side by side)

-- 💡 KEY MESSAGE: "Your data engineers use SQL. Your data scientists 
--  use Python. Both work on the same platform, same data, same governance."


-- =============================================================
-- [6] HORIZON — GOVERNANCE, TAGS, MASKING (Minute 7–9)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Governance is built-in, not bolted on. Snowflake Horizon provides 
--  unified discovery, classification, masking, and lineage — all 
--  from a single control plane."

-- 6a. Create a table with sensitive data
CREATE OR REPLACE TABLE tml_driver_profiles (
    driver_id    VARCHAR,
    driver_name  VARCHAR,
    phone        VARCHAR,
    license_no   VARCHAR,
    vin          VARCHAR,
    dealer_region VARCHAR
);

INSERT INTO tml_driver_profiles VALUES
  ('TML-D-101', 'Rahul Sharma',  '+91-9876543210', 'MH-12-2024-0012345', 'MAT6NEXEV2024001', 'Pune-Chikhali'),
  ('TML-D-102', 'Priya Patel',   '+91-8765432109', 'GJ-01-2023-0098765', 'MAT6HARRI2023045', 'Ahmedabad-Sanand'),
  ('TML-D-103', 'Amit Singh',    '+91-7654321098', 'DL-05-2025-0054321', 'MAT6SAFAR2025012', 'Lucknow-Pantnagar');

-- 6b. Dynamic Masking Policies — auto-masks for non-privileged roles
CREATE OR REPLACE MASKING POLICY mask_phone AS (val VARCHAR) 
  RETURNS VARCHAR ->
  CASE 
    WHEN CURRENT_ROLE() IN ('SYSADMIN','ACCOUNTADMIN') THEN val
    ELSE CONCAT('XXX-XXX-', RIGHT(val, 4))
  END;

CREATE OR REPLACE MASKING POLICY mask_license AS (val VARCHAR) 
  RETURNS VARCHAR ->
  CASE 
    WHEN CURRENT_ROLE() IN ('SYSADMIN','ACCOUNTADMIN') THEN val
    ELSE '****-MASKED-****'
  END;

CREATE OR REPLACE MASKING POLICY mask_vin AS (val VARCHAR) 
  RETURNS VARCHAR ->
  CASE 
    WHEN CURRENT_ROLE() IN ('SYSADMIN','ACCOUNTADMIN') THEN val
    ELSE CONCAT('***VIN-', RIGHT(val, 4))
  END;

-- Apply policies to columns
ALTER TABLE tml_driver_profiles MODIFY COLUMN phone      SET MASKING POLICY mask_phone;
ALTER TABLE tml_driver_profiles MODIFY COLUMN license_no  SET MASKING POLICY mask_license;
ALTER TABLE tml_driver_profiles MODIFY COLUMN vin         SET MASKING POLICY mask_vin;

-- Query — as SYSADMIN you see full data
SELECT * FROM tml_driver_profiles;

-- 🗣 TALK TRACK:
-- "As an admin, I see everything. But when a business analyst with 
--  a restricted role runs this exact same query — phone, license, 
--  and VIN are automatically masked. No code changes. Policy-driven."

-- 6c. Object Tagging — classify sensitive data
CREATE OR REPLACE TAG pii_tag     COMMENT = 'Personally Identifiable Information';
CREATE OR REPLACE TAG sensitivity COMMENT = 'Data sensitivity level';

ALTER TABLE tml_driver_profiles MODIFY COLUMN phone      SET TAG pii_tag = 'PHONE_NUMBER';
ALTER TABLE tml_driver_profiles MODIFY COLUMN license_no  SET TAG pii_tag = 'LICENSE';
ALTER TABLE tml_driver_profiles MODIFY COLUMN vin         SET TAG pii_tag = 'VEHICLE_VIN';
ALTER TABLE tml_driver_profiles MODIFY COLUMN driver_name SET TAG sensitivity = 'HIGH';

-- 🗣 TALK TRACK:
-- "Tags feed into the Governance dashboard. Your compliance team 
--  gets full visibility — where PII lives, who accessed it, 
--  what policies protect it."

-- 👉 [UI] Navigate to: Horizon Catalog → Governance & Security
-- 👉 [UI] Show tagged objects, classification summary
-- 👉 [UI] Use Universal Search (top search bar) — type "tml_driver" 
--         to show object discovery

-- 💡 KEY MESSAGE: "One governance model across all data, all workloads, 
--  all users. Discovery, classification, masking, lineage — unified."


-- =============================================================
-- [7] SECURITY & COMPLIANCE (Minute 9–10)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Beyond masking, Snowflake provides enterprise-grade security: 
--  role-based access, network policies, encryption everywhere, 
--  and a Trust Center for security posture monitoring."

-- 7a. Role-Based Access Control (RBAC)
SHOW ROLES;

-- Show what a role has access to
SHOW GRANTS TO ROLE SYSADMIN;

-- 7b. Row Access Policy — row-level security (concept)
-- 🗣 TALK TRACK:
-- "Row Access Policies restrict which rows a user can see based on 
--  their role. For example, a Pune-Chikhali dealer only sees their region's vehicles."

CREATE OR REPLACE ROW ACCESS POLICY tml_region_filter AS (region_val VARCHAR)
  RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('SYSADMIN','ACCOUNTADMIN') 
  OR region_val = 'Pune-Chikhali';  -- non-admins only see Pune-Chikhali dealer data

-- ALTER TABLE tml_driver_profiles ADD ROW ACCESS POLICY tml_region_filter ON (dealer_region);

-- 7c. Trust Center
-- 👉 [UI] Navigate to: Admin → Security → Trust Center
-- 👉 [UI] Show the security posture scorecard and recommendations

-- 🗣 TALK TRACK:
-- "Trust Center continuously evaluates your account security posture 
--  and gives actionable recommendations. Built-in, no third-party audit 
--  tools needed."

-- 7d. Encryption & compliance summary
-- 🗣 TALK TRACK:
-- "All data is encrypted at rest (AES-256) and in transit (TLS 1.2+) 
--  by default. Business Critical edition adds Tri-Secret Secure 
--  and HIPAA/HITRUST compliance. SOC 2, PCI DSS, ISO 27001 — all covered."

-- 💡 KEY MESSAGE: "Security is not an add-on. It's foundational. 
--  Encryption, RBAC, masking, row policies, Trust Center — all built in."


-- =============================================================
-- [8] CORTEX AI — LLMs, ML, AGENTS, CHAT APIs (Minute 10–12)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Now the AI layer — Cortex AI. LLMs running as SQL functions 
--  inside Snowflake. No data movement. No external API calls. 
--  Your governance policies apply automatically to AI workloads."

-- 8a. Sample data — customer service feedback
CREATE OR REPLACE TABLE tml_service_feedback (
    ticket_id      VARCHAR,
    vehicle_id     VARCHAR,
    vehicle_model  VARCHAR,
    dealer_code    VARCHAR,
    feedback       VARCHAR,
    language       VARCHAR DEFAULT 'en'
);

INSERT INTO tml_service_feedback (ticket_id, vehicle_id, vehicle_model, dealer_code, feedback) VALUES
  ('TML-TKT-001', 'NEXON-EV-1001', 'Nexon EV Max', 'DLR-PUN-042', 'The new Nexon EV Max infotainment system with Arcade.ev is excellent. Navigation is smooth and the connected car features via ZConnect are very responsive. Very happy with the OTA update.'),
  ('TML-TKT-002', 'HARRIER-2001', 'Harrier Adventure', 'DLR-MUM-018', 'Terrible experience at the Tata Motors service center. Waited 4 hours for Harrier annual service and the AC issue was not resolved. Very disappointed with the service quality.'),
  ('TML-TKT-003', 'SAFARI-3001', 'Safari Adventure Plus', 'DLR-DEL-007', 'Safari battery drains faster than expected in Delhi winter. Otherwise the ADAS features are impressive. Lane departure warning works great on highways but range anxiety is real for long NH drives.');

-- 8b. SENTIMENT ANALYSIS — one line of SQL
SELECT 
    ticket_id,
    vehicle_id,
    vehicle_model,
    dealer_code,
    feedback,
    SNOWFLAKE.CORTEX.SENTIMENT(feedback) AS sentiment_score
FROM tml_service_feedback;

-- 🗣 TALK TRACK:
-- "Sentiment scores in one line of SQL. Positive = high, negative = low. 
--  Run this across millions of service tickets without moving data."

-- 👉 [UI] Click Chart button → show bar chart of sentiment by ticket

-- 8c. SUMMARIZATION — condense long text
SELECT 
    ticket_id,
    vehicle_model,
    SNOWFLAKE.CORTEX.SUMMARIZE(feedback) AS summary
FROM tml_service_feedback;

-- 🗣 TALK TRACK:
-- "Auto-summarize dealer reports, customer complaints, service logs — 
--  at scale. Perfect for TML executive dashboards and dealer scorecards."

-- 8d. TRANSLATION — multilingual support
SELECT 
    ticket_id,
    feedback AS original_english,
    SNOWFLAKE.CORTEX.TRANSLATE(feedback, 'en', 'hi') AS hindi,
    SNOWFLAKE.CORTEX.TRANSLATE(feedback, 'en', 'mr') AS marathi
FROM tml_service_feedback
WHERE ticket_id = 'TML-TKT-001';

-- 🗣 TALK TRACK:
-- "Built-in translation across 12+ languages. For Tata Motors — 
--  analyze dealer feedback across India in Hindi, Marathi, Tamil, 
--  and global markets without third-party translation services."

-- 8e. COMPLETE — LLM-powered analysis (Chat API)
SELECT 
    ticket_id,
    vehicle_model,
    feedback,
    SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b',
      'You are a Tata Motors service analyst. Based on this customer 
       feedback for a ' || vehicle_model || ', suggest one specific 
       action the Tata Motors service team should take. Be concise 
       in 2 sentences: ' || feedback
    ) AS recommended_action
FROM tml_service_feedback;

-- 🗣 TALK TRACK:
-- "We just asked an LLM to recommend actions for each ticket — all 
--  inside Snowflake. This is the same Chat API your developers 
--  would call from custom apps. REST endpoint or SQL — same model, 
--  same security perimeter."

-- 8f. ML FUNCTIONS — built-in anomaly detection & forecasting (concept)
-- 🗣 TALK TRACK:
-- "Beyond LLMs, Snowflake has built-in ML functions for forecasting 
--  and anomaly detection — no model training infrastructure needed."

-- Example concept (requires time-series data):
-- CREATE SNOWFLAKE.ML.ANOMALY_DETECTION my_model(...)
-- SELECT * FROM TABLE(my_model!DETECT_ANOMALIES(...));

-- 💡 KEY MESSAGE: "AI is not a separate tool. It's a SQL function. 
--  Same governance, same platform, same security."


-- =============================================================
-- [9] MODELS — MODEL REGISTRY & FINE-TUNING (Minute 12)
-- =============================================================
-- 🗣 TALK TRACK:
-- "For teams building custom ML models, Snowflake has a Model Registry — 
--  version, manage, and deploy models centrally. Plus Cortex Fine-tuning 
--  to customize LLMs on your data without expensive training infrastructure."

-- 👉 [UI] Navigate to: AI & ML → Models (show the registry page)

-- Show models in the account
SHOW MODELS;

-- 🗣 TALK TRACK:
-- "Register a scikit-learn, PyTorch, or XGBoost model. Version it. 
--  Deploy it. Call it from SQL. One central place for your ML lifecycle."

-- Fine-tuning concept:
-- SELECT SNOWFLAKE.CORTEX.FINETUNE(
--   'CREATE', 'my_custom_model',
--   'llama3.1-8b',
--   'SELECT prompt, completion FROM training_data');

-- 💡 KEY MESSAGE: "Train, register, deploy, govern — all in one platform."


-- =============================================================
-- [10] STUDIO — MARKETPLACE & DATA SHARING (Minute 12–13)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Snowflake Marketplace lets you access 2,000+ live datasets — 
--  weather, geospatial, financial, demographics — with zero pipelines. 
--  Data is shared in place. Always current. No copies."

-- 👉 [UI] Navigate to: Marketplace
-- 👉 [UI] Search for something relevant (e.g., "India roads" or 
--         "weather India" or "vehicle telematics")
-- 👉 [UI] Click a free listing → show "Get" button
-- 👉 [UI] Show Provider Studio (Data Sharing → Provider Studio)

-- 🗣 TALK TRACK:
-- "And it's two-way. Tata Motors can also PUBLISH data products — share 
--  with dealers, TASS (Tata AutoComp), JLR, or fleet partners. Live, 
--  governed, no file transfers."

-- Data Sharing concept:
-- CREATE SHARE tml_dealer_share;
-- GRANT USAGE ON DATABASE TML_PLATFORM_DEMO TO SHARE tml_dealer_share;

-- 💡 KEY MESSAGE: "Marketplace = data as a service. No APIs, no ETL, 
--  no stale extracts. Live data, always fresh."


-- =============================================================
-- [11] ADMINISTRATION & COST MANAGEMENT (Minute 13–14)
-- =============================================================
-- 🗣 TALK TRACK:
-- "Full observability and cost control — built in. No external 
--  monitoring stack needed."

-- 11a. Credit consumption by warehouse
SELECT 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED)                AS total_credits,
    SUM(CREDITS_USED_COMPUTE)        AS compute_credits,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_service_credits
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
    DATE_RANGE_END   => CURRENT_DATE()
))
GROUP BY WAREHOUSE_NAME
ORDER BY total_credits DESC;

-- 👉 [UI] Click Chart button → show bar chart of credits by warehouse

-- 11b. Recent query performance
SELECT 
    QUERY_ID,
    QUERY_TYPE,
    WAREHOUSE_NAME,
    USER_NAME,
    EXECUTION_STATUS,
    TOTAL_ELAPSED_TIME/1000     AS elapsed_seconds,
    BYTES_SCANNED/1024/1024     AS mb_scanned,
    ROWS_PRODUCED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 10))
ORDER BY START_TIME DESC;

-- 👉 [UI] Navigate to: Monitoring → Query History
-- 👉 [UI] Click a query → show Query Profile (execution plan)
-- 👉 [UI] Navigate to: Admin → Cost Management → show dashboards
-- 👉 [UI] Mention: Resource Monitors and Budgets for spend caps

-- 🗣 TALK TRACK:
-- "See exactly which teams, warehouses, and queries drive cost. 
--  Set budgets, alerts, and resource monitors to cap spend 
--  automatically. Full transparency."

-- 💡 KEY MESSAGE: "No surprise bills. Complete cost visibility 
--  and control at every level."


-- =============================================================
-- [12] SNOWGRID — CROSS-REGION & CROSS-CLOUD (Minute 14–14.5)
-- =============================================================
-- 🗣 TALK TRACK:
-- "The outermost layer — Snowgrid. This is the fabric that connects 
--  Snowflake across AWS, Azure, and GCP — across any region globally."

-- 👉 [UI] Navigate to: Admin → Accounts → Replication
-- 👉 [UI] Show replication groups if available

-- Replication concept:
-- ALTER DATABASE TML_PLATFORM_DEMO ENABLE REPLICATION TO ACCOUNTS tml_org.tml_dr_account;

-- 🗣 TALK TRACK:
-- "Replicate databases across regions for disaster recovery. 
--  Failover in minutes, not hours. Share data cross-cloud without 
--  copying. A truly global data platform."

-- Failover concept (Business Critical+):
-- ALTER DATABASE TML_PLATFORM_DEMO ENABLE FAILOVER TO ACCOUNTS tml_org.tml_dr_account;

-- 💡 KEY MESSAGE: "Multi-cloud, multi-region. One platform. 
--  Global replication, sharing, and DR — all built in."


-- =============================================================
-- [13] CORTEX CODE — AI ASSISTANT (Minute 14.5–15)
-- =============================================================
-- 🗣 TALK TRACK:
-- "One last thing — Cortex Code. An AI coding assistant embedded 
--  right here in Snowsight. Your team asks questions in plain 
--  English and gets SQL or Python generated instantly."

-- 👉 [UI — LIVE DEMO, NOT SQL]
-- Step 1: Click the Cortex Code sparkle icon (bottom-right in Workspaces)
-- Step 2: Type: "Find all Tata Motors vehicles with overspeed events from 
--          tml_vehicle_telemetry and show their plant region"
-- Step 3: Show generated SQL → Run it → show results
-- Step 4: Try: "Create a chart of sentiment scores from tml_service_feedback by vehicle model"

-- 🗣 TALK TRACK:
-- "No context-switching to external AI tools. Cortex Code understands 
--  your schema, your policies, your data. Everything stays within 
--  your Snowflake security perimeter."

-- 💡 KEY MESSAGE: "AI-powered development, inside the same 
--  platform where you build and run workloads."


-- =============================================================
-- [14] CLOSING — WRAP UP
-- =============================================================
-- 🗣 TALK TRACK:
-- "So let's recap. In one browser session, we just walked through 
--  every layer of the Snowflake platform:
--
--  ┌──────────────────────────────────────────────────┐
--  │  ✅ Elastic Compute — CPU, GPU, Containers       │
--  │  ✅ Interoperable Storage — All 3 data types     │
--  │  ✅ Managed Metadata — Auto-stats, caching       │
--  │  ✅ Multi-Language — SQL + Python natively        │
--  │  ✅ Horizon Governance — Tags, masking, lineage   │
--  │  ✅ Security — RBAC, row policies, Trust Center   │
--  │  ✅ Cortex AI — Sentiment, summary, LLMs         │
--  │  ✅ Model Registry — ML lifecycle                 │
--  │  ✅ Marketplace — 2000+ live datasets             │
--  │  ✅ Cost Management — Full visibility & control   │
--  │  ✅ Snowgrid — Cross-cloud, cross-region          │
--  │  ✅ Cortex Code — AI coding assistant             │
--  └──────────────────────────────────────────────────┘
--
--  We didn't install anything. We didn't configure infrastructure. 
--  We didn't leave the browser. 
--
--  One platform. Data + AI + Apps + Governance. That's Snowflake."


-- =============================================================
-- 🧹 [15] CLEANUP — Run after demo if needed
-- =============================================================
-- Uncomment and run to clean up all demo objects:

-- DROP TABLE IF EXISTS tml_vehicle_telemetry;
-- DROP TABLE IF EXISTS tml_driver_profiles;
-- DROP TABLE IF EXISTS tml_service_feedback;
-- DROP FUNCTION IF EXISTS kmh_to_mph(FLOAT);
-- DROP MASKING POLICY IF EXISTS mask_phone;
-- DROP MASKING POLICY IF EXISTS mask_license;
-- DROP MASKING POLICY IF EXISTS mask_vin;
-- DROP ROW ACCESS POLICY IF EXISTS tml_region_filter;
-- DROP TAG IF EXISTS pii_tag;
-- DROP TAG IF EXISTS sensitivity;
-- DROP COMPUTE POOL IF EXISTS TML_GPU_POOL;
-- DROP DATABASE IF EXISTS TML_PLATFORM_DEMO;
-- DROP WAREHOUSE IF EXISTS TML_ANALYTICS_WH;