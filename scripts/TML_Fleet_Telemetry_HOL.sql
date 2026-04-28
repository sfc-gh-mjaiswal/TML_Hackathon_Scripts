/***********************************************************************
  SNOWFLAKE HANDS-ON LAB — TATA MOTORS FLEET TELEMETRY
  ---------------------------------------------------------------
  Duration  : 30 minutes
  Audience  : Data Engineers, Analysts, Architects
  Interface : Snowsight Worksheet

  DATABASES:
  • TML_FLEET_TELEMETRY       — SHARED (read-only), provided by data team
  • TML_FLEET_TELEMETRY_HOL   — YOUR lab sandbox (created in Step 1)

  All CREATE statements target TML_FLEET_TELEMETRY_HOL.
  All SELECT queries read from the shared TML_FLEET_TELEMETRY.

  MODULES:
  ┌──────────────────────────────────────────────────────────┐
  │  [1]  SETUP & ARCHITECTURE TOUR          (2 min)        │
  │  [2]  TRACK-AND-TRACE — One Vehicle E2E  (3 min)        │
  │  [3]  RAW TELEMETRY EXPLORATION          (2 min)        │
  │  [4]  CURATED LAYER — Signal-Split Views (2 min)        │
  │  [5]  BUILD: VIEWS, UDFs & SPs           (5 min)        │
  │  [6]  BUSINESS-READY ANALYTICS           (2 min)        │
  │  [7]  GEOSPATIAL — H3 & ROAD SEGMENTS   (3 min)        │
  │  [8]  FLEET HEALTH & VEHICLE 360         (2 min)        │
  │  [9]  GOVERNANCE — ROW & COLUMN POLICIES (5 min)        │
  │  [10] CORTEX AI — LLM-POWERED INSIGHTS  (3 min)        │
  │  [11] CORTEX SEARCH — SEMANTIC SEARCH   (1 min)        │
  │  [12] REAL-TIME FLEET STATUS + COST MODEL(1 min)        │
  │  [13] CLEANUP                            (1 min)        │
  │  [14] DISCUSSION & LIVE Q&A              (1 min)        │
  └──────────────────────────────────────────────────────────┘

  PRE-REQUISITES:
  • Role: ACCOUNTADMIN
  • Warehouse: TML_HACKATHON (active)
  • Shared database TML_FLEET_TELEMETRY accessible
***********************************************************************/


-- =============================================================
-- [1] SETUP & ARCHITECTURE TOUR (2 min)
-- =============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TML_HACKATHON;

-- Create your HOL sandbox database (all lab objects go here)
CREATE DATABASE IF NOT EXISTS TML_FLEET_TELEMETRY_HOL
    COMMENT = 'Hands-On Lab sandbox — views, UDFs, SPs built on top of shared TML_FLEET_TELEMETRY';
---Senthil to add input shared database---

---Senthil to add input shared database---
CREATE SCHEMA IF NOT EXISTS TML_FLEET_TELEMETRY_HOL.CURATED;
CREATE SCHEMA IF NOT EXISTS TML_FLEET_TELEMETRY_HOL.BUSINESSREADY;

-- Explore the shared database architecture
SHOW SCHEMAS IN DATABASE TML_FLEET_TELEMETRY;

-- RAW           → Ingested via Snowpipe Streaming SDK from Kafka (5M+ events)
-- CURATED       → Signal-split views: GPS, IMU, J1939, Telematics
-- BUSINESSREADY → Trip summaries, fleet health, Vehicle360, Customer360
-- DASHCAM       → Dashcam events + Cortex Search services
-- MASTER        → Geofences, road segments, customer retention policies
-- COST_MODEL    → TCO benchmark assumptions

-- What tables and views exist in the shared database?
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT
FROM TML_FLEET_TELEMETRY.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME;


-- =============================================================
-- [2] TRACK-AND-TRACE — FOLLOW ONE VEHICLE ACROSS ALL LAYERS (3 min)
-- =============================================================
-- Pick VH_0000100 and trace it from raw ingestion → curated → business-ready

-- Step 1: RAW — raw telemetry events as they landed from Kafka
SELECT
    EVENT_TS,
    VEHICLE_ID,
    CUSTOMER_ID,
    GPS_LATITUDE,
    GPS_LONGITUDE,
    GPS_SPEED_KPH,
    J1939_ENGINE_RPM,
    J1939_FUEL_RATE_LPH,
    J1939_COOLANT_TEMP_C,
    TEL_IGNITION_STATUS,
    INGEST_TS
FROM TML_FLEET_TELEMETRY.RAW.VEHICLE_TELEMETRY_RAW_V1
WHERE VEHICLE_ID = 'VH_0000100'
ORDER BY EVENT_TS
LIMIT 20;

-- Step 2: CURATED — GPS signal view (cleaned, with late-arrival flag)
SELECT
    EVENT_TS,
    VEHICLE_ID,
    GPS_LATITUDE,
    GPS_LONGITUDE,
    GPS_SPEED_KPH,
    GPS_HEADING_DEG,
    ARRIVED_LATE
FROM TML_FLEET_TELEMETRY.CURATED.VW_GPS_SIGNALS_V1
WHERE VEHICLE_ID = 'VH_0000100'
ORDER BY EVENT_TS
LIMIT 20;

-- Step 3: CURATED — J1939 engine signals for the same vehicle
SELECT
    EVENT_TS,
    VEHICLE_ID,
    J1939_ENGINE_RPM,
    J1939_FUEL_RATE_LPH,
    J1939_COOLANT_TEMP_C,
    J1939_ACTIVE_DTC_COUNT,
    J1939_ODOMETER_KM
FROM TML_FLEET_TELEMETRY.CURATED.VW_J1939_SIGNALS_V1
WHERE VEHICLE_ID = 'VH_0000100'
ORDER BY EVENT_TS
LIMIT 20;

-- Step 4: BUSINESSREADY — trip summaries generated from the curated data
SELECT
    VEHICLE_ID,
    TRIP_NUM,
    TRIP_START_TS,
    TRIP_END_TS,
    ROUND(DURATION_MIN, 1)                AS mins,
    ROUND(DISTANCE_KM, 1)                 AS km,
    ROUND(FUEL_CONSUMED_L, 1)             AS fuel_l,
    ROUND(FUEL_EFFICIENCY_L_PER_100KM, 2) AS fuel_eff,
    START_GEOFENCE,
    END_GEOFENCE
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.TRIP_SUMMARIES_V1
WHERE VEHICLE_ID = 'VH_0000100'
ORDER BY TRIP_START_TS;

-- Step 5: BUSINESSREADY — fleet health status for this vehicle
SELECT
    VEHICLE_ID,
    HEALTH_STATUS,
    MAX_DTC_COUNT,
    HAS_CAN_BUS_ISSUES,
    ROUND(MAX_COOLANT_TEMP_C, 1) AS max_coolant_c,
    ROUND(CURRENT_ODOMETER_KM, 0) AS odometer_km
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.FLEET_HEALTH_DASHBOARD_V1
WHERE VEHICLE_ID = 'VH_0000100';


-- =============================================================
-- [3] RAW TELEMETRY EXPLORATION (2 min)
-- =============================================================

-- 3a. How much data? (Kafka → Snowpipe Streaming → this table)
SELECT COUNT(*) AS total_events FROM TML_FLEET_TELEMETRY.RAW.VEHICLE_TELEMETRY_RAW_V1;

-- 3b. Preview raw events — one row = one second of vehicle data
SELECT
    EVENT_TS,
    CUSTOMER_ID,
    VEHICLE_ID,
    GPS_LATITUDE,
    GPS_LONGITUDE,
    GPS_SPEED_KPH,
    J1939_ENGINE_RPM,
    J1939_FUEL_RATE_LPH,
    J1939_COOLANT_TEMP_C,
    J1939_ACTIVE_DTC_COUNT,
    TEL_IGNITION_STATUS,
    TEL_CAN_BUS_HEALTH
FROM TML_FLEET_TELEMETRY.RAW.VEHICLE_TELEMETRY_RAW_V1
LIMIT 20;

-- 3c. Fleet-wide stats in one query
SELECT
    COUNT(DISTINCT CUSTOMER_ID) AS customers,
    COUNT(DISTINCT VEHICLE_ID)  AS vehicles,
    COUNT(DISTINCT DEVICE_ID)   AS devices,
    MIN(EVENT_TS)               AS earliest_event,
    MAX(EVENT_TS)               AS latest_event
FROM TML_FLEET_TELEMETRY.RAW.VEHICLE_TELEMETRY_RAW_V1;

-- 3d. Dead Letter Queue — what happens when ingestion fails?
SELECT
    ERROR_MESSAGE,
    SOURCE_TOPIC,
    SOURCE_PARTITION,
    DLQ_INGEST_TS,
    RECORD_METADATA:key::VARCHAR AS vehicle_key
FROM TML_FLEET_TELEMETRY.RAW.DLQ_MESSAGES
LIMIT 10;


-- =============================================================
-- [4] CURATED LAYER — Signal-Split Views (2 min)
-- =============================================================

-- 4a. GPS Signals — position, speed, heading + late-arrival flag
SELECT
    EVENT_TS,
    VEHICLE_ID,
    GPS_LATITUDE,
    GPS_LONGITUDE,
    GPS_SPEED_KPH,
    GPS_HEADING_DEG,
    GPS_SATELLITE_COUNT,
    ARRIVED_LATE
FROM TML_FLEET_TELEMETRY.CURATED.VW_GPS_SIGNALS_V1
WHERE ARRIVED_LATE = FALSE
LIMIT 20;

-- 4b. J1939 Engine/CAN Bus Signals — RPM, fuel, temp, DTCs
SELECT
    EVENT_TS,
    VEHICLE_ID,
    J1939_ENGINE_RPM,
    J1939_ENGINE_LOAD_PCT,
    J1939_FUEL_RATE_LPH,
    J1939_COOLANT_TEMP_C,
    J1939_BATTERY_VOLTAGE_V,
    J1939_ACTIVE_DTC_COUNT,
    J1939_BRAKE_SWITCH
FROM TML_FLEET_TELEMETRY.CURATED.VW_J1939_SIGNALS_V1
WHERE J1939_ENGINE_RPM > 2000
LIMIT 20;

-- 4c. IMU Signals — harsh driving detection via accelerometer/gyro
SELECT
    EVENT_TS,
    VEHICLE_ID,
    IMU_ACCEL_X_G,
    IMU_ACCEL_Y_G,
    IMU_ACCEL_Z_G,
    IMU_GYRO_X_DPS,
    IMU_GYRO_Y_DPS,
    IMU_GYRO_Z_DPS
FROM TML_FLEET_TELEMETRY.CURATED.VW_IMU_SIGNALS_V1
WHERE ABS(IMU_ACCEL_X_G) > 0.3
LIMIT 20;

-- 4d. Telematics Device Health — connectivity, CAN bus, tamper alerts
SELECT
    EVENT_TS,
    VEHICLE_ID,
    TEL_IGNITION_STATUS,
    TEL_EXTERNAL_POWER_V,
    TEL_GSM_SIGNAL_DBM,
    TEL_NETWORK_TYPE,
    TEL_CAN_BUS_HEALTH,
    TEL_TAMPER_ALERT
FROM TML_FLEET_TELEMETRY.CURATED.VW_TELEMATICS_V1
WHERE TEL_CAN_BUS_HEALTH != 'OK'
   OR TEL_TAMPER_ALERT = TRUE
LIMIT 20;

-- 4e. Data quality — how many late-arriving events?
SELECT
    ARRIVED_LATE,
    COUNT(*) AS event_count
FROM TML_FLEET_TELEMETRY.CURATED.VW_GPS_SIGNALS_V1
GROUP BY ARRIVED_LATE;


-- =============================================================
-- [5] BUILD: VIEWS, UDFs & STORED PROCEDURES (5 min)
-- =============================================================
-- All objects created in TML_FLEET_TELEMETRY_HOL (your sandbox).
-- They read from the shared TML_FLEET_TELEMETRY database.

-- ── 5a. UDF: Haversine distance between two GPS points (km) ──
CREATE OR REPLACE FUNCTION TML_FLEET_TELEMETRY_HOL.CURATED.HAVERSINE_KM(
    LAT1 FLOAT, LON1 FLOAT, LAT2 FLOAT, LON2 FLOAT
)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    6371 * ACOS(
        LEAST(1, GREATEST(-1,
            COS(RADIANS(LAT1)) * COS(RADIANS(LAT2))
            * COS(RADIANS(LON2) - RADIANS(LON1))
            + SIN(RADIANS(LAT1)) * SIN(RADIANS(LAT2))
        ))
    )
$$;

-- Test it: Bangalore → Hyderabad
SELECT ROUND(
    TML_FLEET_TELEMETRY_HOL.CURATED.HAVERSINE_KM(12.9716, 77.5946, 17.385, 78.4867)
, 1) AS blr_to_hyd_km;

-- ── 5b. UDF: Classify engine health from sensor readings ──
CREATE OR REPLACE FUNCTION TML_FLEET_TELEMETRY_HOL.CURATED.ENGINE_HEALTH_GRADE(
    COOLANT_TEMP_C FLOAT,
    DTC_COUNT NUMBER,
    BATTERY_V FLOAT,
    OIL_PRESSURE_KPA FLOAT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE
        WHEN DTC_COUNT >= 3 OR COOLANT_TEMP_C > 105 OR BATTERY_V < 22
            THEN 'CRITICAL'
        WHEN DTC_COUNT >= 1 OR COOLANT_TEMP_C > 95 OR BATTERY_V < 24 OR OIL_PRESSURE_KPA < 200
            THEN 'WARNING'
        ELSE 'HEALTHY'
    END
$$;

-- Test it against live J1939 data from the shared database
SELECT
    VEHICLE_ID,
    J1939_COOLANT_TEMP_C,
    J1939_ACTIVE_DTC_COUNT,
    J1939_BATTERY_VOLTAGE_V,
    J1939_OIL_PRESSURE_KPA,
    TML_FLEET_TELEMETRY_HOL.CURATED.ENGINE_HEALTH_GRADE(
        J1939_COOLANT_TEMP_C,
        J1939_ACTIVE_DTC_COUNT,
        J1939_BATTERY_VOLTAGE_V,
        J1939_OIL_PRESSURE_KPA
    ) AS engine_grade
FROM TML_FLEET_TELEMETRY.CURATED.VW_J1939_SIGNALS_V1
LIMIT 20;

-- ── 5c. VIEW: Driver Scorecard ──
-- Joins trip summaries with fleet health to grade each vehicle/driver.
CREATE OR REPLACE VIEW TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.DRIVER_SCORECARD_V1 AS
SELECT
    t.CUSTOMER_ID,
    t.VEHICLE_ID,
    COUNT(*)                                          AS TOTAL_TRIPS,
    ROUND(AVG(t.FUEL_EFFICIENCY_L_PER_100KM), 2)     AS AVG_FUEL_EFF,
    ROUND(AVG(t.AVG_SPEED_KPH), 1)                   AS AVG_SPEED,
    ROUND(MAX(t.MAX_SPEED_KPH), 1)                   AS TOP_SPEED,
    ROUND(SUM(t.DISTANCE_KM), 1)                     AS TOTAL_KM,
    ROUND(SUM(t.FUEL_CONSUMED_L), 1)                 AS TOTAL_FUEL_L,
    fh.HEALTH_STATUS,
    fh.MAX_DTC_COUNT,
    CASE
        WHEN AVG(t.FUEL_EFFICIENCY_L_PER_100KM) < 25 AND fh.MAX_DTC_COUNT = 0
            THEN 'A - Excellent'
        WHEN AVG(t.FUEL_EFFICIENCY_L_PER_100KM) < 35 AND fh.MAX_DTC_COUNT <= 1
            THEN 'B - Good'
        WHEN AVG(t.FUEL_EFFICIENCY_L_PER_100KM) < 45
            THEN 'C - Needs Improvement'
        ELSE 'D - At Risk'
    END AS DRIVER_GRADE
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.TRIP_SUMMARIES_V1 t
LEFT JOIN TML_FLEET_TELEMETRY.BUSINESSREADY.FLEET_HEALTH_DASHBOARD_V1 fh
    ON t.VEHICLE_ID = fh.VEHICLE_ID AND t.CUSTOMER_ID = fh.CUSTOMER_ID
GROUP BY t.CUSTOMER_ID, t.VEHICLE_ID, fh.HEALTH_STATUS, fh.MAX_DTC_COUNT;

-- Query the scorecard
SELECT *
FROM TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.DRIVER_SCORECARD_V1
ORDER BY DRIVER_GRADE, TOTAL_KM DESC
LIMIT 20;

-- Grade distribution
SELECT DRIVER_GRADE, COUNT(*) AS vehicle_count
FROM TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.DRIVER_SCORECARD_V1
GROUP BY DRIVER_GRADE
ORDER BY DRIVER_GRADE;

-- ── 5d. VIEW: Speed Alert Report — trips exceeding threshold ──
CREATE OR REPLACE VIEW TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.SPEED_ALERT_REPORT_V1 AS
SELECT
    CUSTOMER_ID,
    VEHICLE_ID,
    TRIP_NUM,
    TRIP_START_TS,
    START_GEOFENCE,
    END_GEOFENCE,
    ROUND(MAX_SPEED_KPH, 1)                AS MAX_SPEED_KPH,
    ROUND(AVG_SPEED_KPH, 1)                AS AVG_SPEED_KPH,
    ROUND(DISTANCE_KM, 1)                  AS DISTANCE_KM,
    ROUND(DURATION_MIN, 1)                 AS DURATION_MIN,
    CASE
        WHEN MAX_SPEED_KPH > 100 THEN 'CRITICAL'
        WHEN MAX_SPEED_KPH > 80  THEN 'HIGH'
        WHEN MAX_SPEED_KPH > 60  THEN 'MODERATE'
        ELSE 'NORMAL'
    END AS SPEED_ALERT_LEVEL
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.TRIP_SUMMARIES_V1
WHERE MAX_SPEED_KPH > 60;

-- Query speed alerts
SELECT *
FROM TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.SPEED_ALERT_REPORT_V1
ORDER BY MAX_SPEED_KPH DESC
LIMIT 20;

-- Alert distribution
SELECT SPEED_ALERT_LEVEL, COUNT(*) AS alert_count
FROM TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.SPEED_ALERT_REPORT_V1
GROUP BY SPEED_ALERT_LEVEL
ORDER BY alert_count DESC;

-- ── 5e. STORED PROCEDURE: Fleet Summary Report for a Customer ──
CREATE OR REPLACE PROCEDURE TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.GET_CUSTOMER_FLEET_REPORT(
    P_CUSTOMER_ID VARCHAR
)
RETURNS TABLE(
    VEHICLE_ID          VARCHAR,
    TOTAL_TRIPS         NUMBER,
    TOTAL_DISTANCE_KM   FLOAT,
    TOTAL_FUEL_L        FLOAT,
    AVG_FUEL_EFFICIENCY FLOAT,
    TOP_SPEED           FLOAT,
    HEALTH_STATUS       VARCHAR,
    DRIVER_GRADE        VARCHAR
)
LANGUAGE SQL
AS
DECLARE
    res RESULTSET;
BEGIN
    res := (SELECT
        VEHICLE_ID,
        TOTAL_TRIPS,
        TOTAL_KM        AS TOTAL_DISTANCE_KM,
        TOTAL_FUEL_L,
        AVG_FUEL_EFF    AS AVG_FUEL_EFFICIENCY,
        TOP_SPEED,
        HEALTH_STATUS,
        DRIVER_GRADE
    FROM TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.DRIVER_SCORECARD_V1
    WHERE CUSTOMER_ID = :P_CUSTOMER_ID
    ORDER BY TOTAL_KM DESC);
    RETURN TABLE(res);
END;

-- Call the procedure
CALL TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.GET_CUSTOMER_FLEET_REPORT('CUST_0001');

-- ── 5f. STORED PROCEDURE: Speed Violation Summary by Customer ──
CREATE OR REPLACE PROCEDURE TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.GET_SPEED_VIOLATIONS(
    P_CUSTOMER_ID VARCHAR,
    P_SPEED_THRESHOLD FLOAT
)
RETURNS TABLE(
    VEHICLE_ID       VARCHAR,
    TRIP_NUM         NUMBER,
    TRIP_START_TS    TIMESTAMP_NTZ,
    MAX_SPEED_KPH   FLOAT,
    START_GEOFENCE   VARCHAR,
    END_GEOFENCE     VARCHAR
)
LANGUAGE SQL
AS
DECLARE
    res RESULTSET;
BEGIN
    res := (SELECT
        VEHICLE_ID,
        TRIP_NUM,
        TRIP_START_TS,
        ROUND(MAX_SPEED_KPH, 1) AS MAX_SPEED_KPH,
        START_GEOFENCE,
        END_GEOFENCE
    FROM TML_FLEET_TELEMETRY.BUSINESSREADY.TRIP_SUMMARIES_V1
    WHERE CUSTOMER_ID = :P_CUSTOMER_ID
      AND MAX_SPEED_KPH > :P_SPEED_THRESHOLD
    ORDER BY MAX_SPEED_KPH DESC);
    RETURN TABLE(res);
END;

-- Call it: find trips > 80 kph for CUST_0100
CALL TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.GET_SPEED_VIOLATIONS('CUST_0100', 80);


-- =============================================================
-- [6] BUSINESS-READY ANALYTICS (2 min)
-- =============================================================

-- 6a. Trip Summaries — automatic trip segmentation with geofencing
SELECT
    CUSTOMER_ID,
    VEHICLE_ID,
    TRIP_NUM,
    TRIP_START_TS,
    TRIP_END_TS,
    ROUND(DURATION_MIN, 1) AS duration_min,
    ROUND(DISTANCE_KM, 1) AS distance_km,
    ROUND(FUEL_CONSUMED_L, 1) AS fuel_l,
    ROUND(FUEL_EFFICIENCY_L_PER_100KM, 2) AS fuel_eff,
    ROUND(AVG_SPEED_KPH, 1) AS avg_speed,
    ROUND(MAX_SPEED_KPH, 1) AS max_speed,
    START_GEOFENCE,
    END_GEOFENCE
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.TRIP_SUMMARIES_V1
ORDER BY DISTANCE_KM DESC
LIMIT 20;

-- 6b. Top 10 longest trips
SELECT
    VEHICLE_ID,
    CUSTOMER_ID,
    TRIP_NUM,
    ROUND(DISTANCE_KM, 1) AS km,
    ROUND(DURATION_MIN, 1) AS mins,
    ROUND(FUEL_EFFICIENCY_L_PER_100KM, 2) AS fuel_eff,
    START_GEOFENCE,
    END_GEOFENCE
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.TRIP_SUMMARIES_V1
ORDER BY DISTANCE_KM DESC
LIMIT 10;

-- 6c. Daily Vehicle Metrics — fuel, speed, brakes, DTCs per vehicle per day
SELECT
    VEHICLE_ID,
    METRIC_DATE,
    TOTAL_READINGS,
    ROUND(AVG_SPEED_KPH, 1)              AS avg_speed,
    ROUND(MAX_SPEED_KPH, 1)              AS max_speed,
    ROUND(DAILY_FUEL_CONSUMED_L, 1)       AS fuel_l,
    ROUND(DAILY_DISTANCE_KM, 1)           AS distance_km,
    BRAKE_EVENTS,
    MAX_DTC_COUNT
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.FACT_VEHICLE_DAILY_METRICS_V1
ORDER BY DAILY_FUEL_CONSUMED_L DESC
LIMIT 20;



-- =============================================================
-- [8] FLEET HEALTH & VEHICLE 360 (2 min)
-- =============================================================

-- 8a. Fleet Health Dashboard — identify at-risk vehicles
SELECT
    VEHICLE_ID,
    CUSTOMER_ID,
    HEALTH_STATUS,
    MINUTES_SINCE_LAST_EVENT,
    MAX_DTC_COUNT,
    HAS_CAN_BUS_ISSUES,
    HAS_TAMPER_ALERT,
    ROUND(MAX_COOLANT_TEMP_C, 1) AS max_coolant_c,
    ROUND(MIN_BATTERY_VOLTAGE_V, 1) AS min_battery_v,
    ROUND(CURRENT_ODOMETER_KM, 0) AS odometer_km
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.FLEET_HEALTH_DASHBOARD_V1
WHERE HEALTH_STATUS IN ('WARNING', 'CRITICAL')
ORDER BY HEALTH_STATUS, MAX_DTC_COUNT DESC
LIMIT 20;

-- 8b. Fleet health breakdown by status
SELECT
    HEALTH_STATUS,
    COUNT(*) AS vehicle_count
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.FLEET_HEALTH_DASHBOARD_V1
GROUP BY HEALTH_STATUS
ORDER BY vehicle_count DESC;

-- 8d. Customer 360 — fleet-level metrics per customer
SELECT
    dc.CUSTOMER_ID,
    dc.CUSTOMER_NAME,
    dc.FLEET_SIZE,
    dc.ACTIVE_VEHICLES,
    dc.TOTAL_EVENTS,
    ROUND(fs.FLEET_AVG_SPEED_KPH, 1)     AS avg_speed,
    ROUND(fs.FLEET_TOTAL_FUEL_L, 0)       AS total_fuel_l,
    fs.FLEET_BRAKE_EVENTS,
    fs.READINGS_WITH_DTC,
    ROUND(fs.FLEET_AVG_COOLANT_TEMP_C, 1) AS avg_coolant_c
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.DIM_CUSTOMER_V1 dc
LEFT JOIN TML_FLEET_TELEMETRY.BUSINESSREADY.FACT_CUSTOMER_FLEET_SUMMARY_V1 fs
    ON dc.CUSTOMER_ID = fs.CUSTOMER_ID
ORDER BY dc.FLEET_SIZE DESC NULLS LAST
LIMIT 10;

-- 8e. Dim Vehicle — master vehicle registry
SELECT
    VEHICLE_ID,
    CUSTOMER_ID,
    DEVICE_ID,
    CUSTOMER_NAME,
    FIRST_SEEN_TS,
    LAST_SEEN_TS,
    TOTAL_GPS_EVENTS
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.DIM_VEHICLE_V1
ORDER BY TOTAL_GPS_EVENTS DESC
LIMIT 20;


-- =============================================================
-- [9] MULTI-TENANT GOVERNANCE — ROW & COLUMN ACCESS (5 min)
-- =============================================================
-- We'll build governance in TML_FLEET_TELEMETRY_HOL:
--   • A local table with customer PII
--   • A ROW ACCESS POLICY  (tenant isolation)
--   • A MASKING POLICY      (column-level PII protection)
--   • Test both as ACCOUNTADMIN and as a customer role

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE TML_HACKATHON;

-- 9a. Create a GOVERNANCE schema in our HOL database
CREATE SCHEMA IF NOT EXISTS TML_FLEET_TELEMETRY_HOL.GOVERNANCE;

-- 9b. Create a local customer-vehicle table with sensitive columns
CREATE OR REPLACE TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES AS
SELECT
    v.CUSTOMER_ID,
    v.CUSTOMER_NAME,
    v.VEHICLE_ID,
    v.DEVICE_ID,
    v.FIRST_SEEN_TS,
    v.LAST_SEEN_TS,
    v.TOTAL_GPS_EVENTS,
    c.RAW_RETENTION_DAYS,
    c.TENANT_ISOLATION_ENABLED,
    'XX-' || UNIFORM(10,99, RANDOM())::VARCHAR || '-XX-'
        || UNIFORM(1000,9999, RANDOM())::VARCHAR AS DEVICE_SERIAL_NUMBER,
    'driver_' || LOWER(REPLACE(v.VEHICLE_ID,'VH_',''))
        || '@tatafleet.com' AS DRIVER_EMAIL
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.DIM_VEHICLE_V1 v
JOIN TML_FLEET_TELEMETRY.MASTER.CUSTOMER_RETENTION_POLICIES c
    ON v.CUSTOMER_ID = c.CUSTOMER_ID;

-- Verify the data
SELECT * FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES LIMIT 10;


-- ── 9c. CREATE MASKING POLICY (column-level access) ──────────
-- Masks DRIVER_EMAIL and DEVICE_SERIAL_NUMBER for non-admin roles.

CREATE OR REPLACE MASKING POLICY TML_FLEET_TELEMETRY_HOL.GOVERNANCE.EMAIL_MASK
    AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
        ELSE REGEXP_REPLACE(val, '.+@', '****@')
    END;

CREATE OR REPLACE MASKING POLICY TML_FLEET_TELEMETRY_HOL.GOVERNANCE.SERIAL_MASK
    AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
        ELSE '****-****'
    END;

-- Apply masking policies to the table columns
ALTER TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES
    MODIFY COLUMN DRIVER_EMAIL
    SET MASKING POLICY TML_FLEET_TELEMETRY_HOL.GOVERNANCE.EMAIL_MASK;

ALTER TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES
    MODIFY COLUMN DEVICE_SERIAL_NUMBER
    SET MASKING POLICY TML_FLEET_TELEMETRY_HOL.GOVERNANCE.SERIAL_MASK;


-- ── 9d. CREATE ROW ACCESS POLICY (tenant isolation) ──────────
-- Each CUST_XXXX_ROLE can only see rows for their own customer.
-- ACCOUNTADMIN sees everything.

CREATE OR REPLACE ROW ACCESS POLICY TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_ROW_POLICY
    AS (CUSTOMER_ID VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ACCOUNTADMIN'
    OR CURRENT_ROLE() = CUSTOMER_ID || '_ROLE';

-- Apply row access policy to the table
ALTER TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES
    ADD ROW ACCESS POLICY TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_ROW_POLICY
    ON (CUSTOMER_ID);


-- ── 9e. GRANT access so customer roles can query the HOL table ──
GRANT USAGE ON DATABASE TML_FLEET_TELEMETRY_HOL TO ROLE CUST_0001_ROLE;
GRANT USAGE ON SCHEMA TML_FLEET_TELEMETRY_HOL.GOVERNANCE TO ROLE CUST_0001_ROLE;
GRANT SELECT ON TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES TO ROLE CUST_0001_ROLE;

GRANT USAGE ON DATABASE TML_FLEET_TELEMETRY_HOL TO ROLE CUST_0002_ROLE;
GRANT USAGE ON SCHEMA TML_FLEET_TELEMETRY_HOL.GOVERNANCE TO ROLE CUST_0002_ROLE;
GRANT SELECT ON TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES TO ROLE CUST_0002_ROLE;


-- ══════════════════════════════════════════════════════════════
-- TEST 1: ACCOUNTADMIN — sees ALL rows, UNMASKED columns
-- ══════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    VEHICLE_ID,
    DRIVER_EMAIL,
    DEVICE_SERIAL_NUMBER,
    TOTAL_GPS_EVENTS
FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES
ORDER BY CUSTOMER_ID
LIMIT 10;

SELECT COUNT(*) AS admin_total_rows
FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES;


-- ══════════════════════════════════════════════════════════════
-- TEST 2: CUST_0001_ROLE — sees ONLY CUST_0001 rows, MASKED PII
-- ══════════════════════════════════════════════════════════════

USE ROLE CUST_0001_ROLE;
USE WAREHOUSE TML_HACKATHON;

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    VEHICLE_ID,
    DRIVER_EMAIL,
    DEVICE_SERIAL_NUMBER,
    TOTAL_GPS_EVENTS
FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES
ORDER BY VEHICLE_ID
LIMIT 10;

SELECT COUNT(*) AS cust_0001_row_count
FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES;

SELECT DISTINCT CUSTOMER_ID
FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES;


-- ══════════════════════════════════════════════════════════════
-- TEST 3: CUST_0002_ROLE — different tenant, same table
-- ══════════════════════════════════════════════════════════════

USE ROLE CUST_0002_ROLE;
USE WAREHOUSE TML_HACKATHON;

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    VEHICLE_ID,
    DRIVER_EMAIL,
    DEVICE_SERIAL_NUMBER
FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES
LIMIT 10;

SELECT COUNT(*) AS cust_0002_row_count
FROM TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES;


-- Switch back to admin for the rest of the lab
USE ROLE ACCOUNTADMIN;

-- 9f. View the existing shared-DB RAP for reference
DESCRIBE ROW ACCESS POLICY TML_FLEET_TELEMETRY.BUSINESSREADY.TRIP_CUSTOMER_RAP;

-- 9g. Retention policies & automated task
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    RAW_RETENTION_DAYS,
    CURATED_RETENTION_DAYS,
    TRIP_RETENTION_DAYS,
    VIDEO_RETENTION_DAYS
FROM TML_FLEET_TELEMETRY.MASTER.CUSTOMER_RETENTION_POLICIES
ORDER BY CUSTOMER_ID
LIMIT 10;

SHOW TASKS IN SCHEMA TML_FLEET_TELEMETRY.RAW;


-- =============================================================
-- [10] CORTEX AI — LLM-POWERED INSIGHTS ON IMAGES (5 min)
-- =============================================================
-- All analysis runs directly on dashcam images stored on a stage.
-- No pre-built event table — the AI sees the images fresh.

-- ── 10a. Setup: Copy images to an SSE-encrypted stage ──────────
-- Cortex COMPLETE with TO_FILE() requires SNOWFLAKE_SSE encryption.
CREATE SCHEMA IF NOT EXISTS TML_FLEET_TELEMETRY_HOL.DASHCAM;

CREATE STAGE IF NOT EXISTS TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

COPY FILES INTO @TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE
FROM @TML_FLEET_TELEMETRY.DASHCAM.DASHCAM_STAGE
PATTERN = '.*\.jpg';

ALTER STAGE TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE REFRESH;

-- 10b. Browse the 25 dashcam images on stage
SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED
FROM DIRECTORY(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE)
WHERE RELATIVE_PATH LIKE '%.jpg'
ORDER BY RELATIVE_PATH;

-- ── 10c. Analyze ALL images with preview URL ───────────────────
-- GET_PRESIGNED_URL generates a clickable link to view the image.
-- Cortex COMPLETE (pixtral-large) analyzes each image via TO_FILE.
SELECT
    d.RELATIVE_PATH AS image_file,
    GET_PRESIGNED_URL(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE, d.RELATIVE_PATH) AS image_preview_url,
    SNOWFLAKE.CORTEX.COMPLETE(
        'pixtral-large',
        'You are a fleet safety analyst for Tata Motors. Analyze this dashcam image. '
        || 'Report: 1) Road conditions 2) Vehicles and objects visible '
        || '3) Safety hazards or concerns. Be concise in 3-4 sentences.',
        TO_FILE('@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE', d.RELATIVE_PATH)
    ) AS image_analysis
FROM DIRECTORY(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE) d
WHERE d.RELATIVE_PATH LIKE '%.jpg'
ORDER BY d.RELATIVE_PATH;

-- ── 10d. Structured JSON extraction — ALL images ───────────────
SELECT
    d.RELATIVE_PATH AS image_file,
    GET_PRESIGNED_URL(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE, d.RELATIVE_PATH) AS image_preview_url,
    SNOWFLAKE.CORTEX.COMPLETE(
        'pixtral-large',
        'Analyze this dashcam image from a Tata Motors fleet vehicle. '
        || 'Return ONLY a JSON object with these fields: '
        || '{"road_condition": "Dry|Wet|Potholed|Under Construction", '
        || '"weather": "Clear|Cloudy|Rain|Fog", '
        || '"visibility": "Good|Moderate|Poor", '
        || '"objects_detected": "comma separated list", '
        || '"safety_risk": "LOW|MEDIUM|HIGH", '
        || '"recommended_action": "one sentence"}',
        TO_FILE('@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE', d.RELATIVE_PATH)
    ) AS structured_analysis
FROM DIRECTORY(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE) d
WHERE d.RELATIVE_PATH LIKE '%.jpg'
ORDER BY d.RELATIVE_PATH;

-- ── 10e. Chained AI: image → description → sentiment ───────────
SELECT
    image_file,
    GET_PRESIGNED_URL(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE, image_file) AS image_preview_url,
    ai_description,
    SNOWFLAKE.CORTEX.SENTIMENT(ai_description) AS safety_sentiment
FROM (
    SELECT
        d.RELATIVE_PATH AS image_file,
        SNOWFLAKE.CORTEX.COMPLETE(
            'pixtral-large',
            'Describe what you see in this dashcam image in 2 sentences. '
            || 'Focus on safety risks and road conditions.',
            TO_FILE('@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE', d.RELATIVE_PATH)
        ) AS ai_description
    FROM DIRECTORY(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE) d
    WHERE d.RELATIVE_PATH LIKE '%.jpg'
)
ORDER BY safety_sentiment ASC;


-- ── 10f. Chained AI: image → description → translate to Hindi ──
SELECT
    d.RELATIVE_PATH AS image_file,
    GET_PRESIGNED_URL(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE, d.RELATIVE_PATH) AS image_preview_url,
    SNOWFLAKE.CORTEX.TRANSLATE(
        SNOWFLAKE.CORTEX.COMPLETE(
            'pixtral-large',
            'Describe this dashcam image in 2 sentences focusing on safety.',
            TO_FILE('@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE', d.RELATIVE_PATH)
        ),
        'en', 'hi'
    ) AS hindi_analysis
FROM DIRECTORY(@TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE) d
WHERE d.RELATIVE_PATH LIKE '%.jpg'
ORDER BY d.RELATIVE_PATH;

-- =============================================================
-- [12] REAL-TIME FLEET STATUS + COST MODEL (1 min)
-- =============================================================

-- 12a. Interactive Table — sub-second lookups for real-time dashboards
SELECT
    VEHICLE_ID,
    CUSTOMER_ID,
    LAST_EVENT_TS,
    ROUND(LATEST_LAT, 4) AS lat,
    ROUND(LATEST_LON, 4) AS lon,
    ROUND(AVG_SPEED_KPH, 1) AS avg_speed,
    ROUND(MAX_SPEED_KPH, 1) AS max_speed,
    IGNITION_STATUS,
    HEALTH_STATUS,
    MAX_DTC_COUNT,
    CAN_BUS_HEALTH
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.REALTIME_FLEET_STATUS
LIMIT 20;

-- 12b. Fleet status breakdown
SELECT
    HEALTH_STATUS,
    IGNITION_STATUS,
    COUNT(*) AS vehicle_count
FROM TML_FLEET_TELEMETRY.BUSINESSREADY.REALTIME_FLEET_STATUS
GROUP BY HEALTH_STATUS, IGNITION_STATUS
ORDER BY vehicle_count DESC;


-- 12d. Credit consumption over the last 7 days
SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_service_credits
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
    DATE_RANGE_END   => CURRENT_DATE()
))
GROUP BY WAREHOUSE_NAME
ORDER BY total_credits DESC;


-- =============================================================
-- [13] CLEANUP (1 min)
-- =============================================================
-- Uncomment below to drop the entire lab sandbox when done:

-- DROP DATABASE IF EXISTS TML_FLEET_TELEMETRY_HOL;

-- Or drop individual objects:
-- DROP STAGE IF EXISTS TML_FLEET_TELEMETRY_HOL.DASHCAM.IMAGE_STAGE;
-- DROP SCHEMA IF EXISTS TML_FLEET_TELEMETRY_HOL.DASHCAM;
-- DROP VIEW IF EXISTS TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.DRIVER_SCORECARD_V1;
-- DROP VIEW IF EXISTS TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.SPEED_ALERT_REPORT_V1;
-- DROP FUNCTION IF EXISTS TML_FLEET_TELEMETRY_HOL.CURATED.HAVERSINE_KM(FLOAT,FLOAT,FLOAT,FLOAT);
-- DROP FUNCTION IF EXISTS TML_FLEET_TELEMETRY_HOL.CURATED.ENGINE_HEALTH_GRADE(FLOAT,NUMBER,FLOAT,FLOAT);
-- DROP PROCEDURE IF EXISTS TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.GET_CUSTOMER_FLEET_REPORT(VARCHAR);
-- DROP PROCEDURE IF EXISTS TML_FLEET_TELEMETRY_HOL.BUSINESSREADY.GET_SPEED_VIOLATIONS(VARCHAR,FLOAT);
-- ALTER TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES DROP ROW ACCESS POLICY TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_ROW_POLICY;
-- ALTER TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES MODIFY COLUMN DRIVER_EMAIL UNSET MASKING POLICY;
-- ALTER TABLE TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES MODIFY COLUMN DEVICE_SERIAL_NUMBER UNSET MASKING POLICY;
-- DROP ROW ACCESS POLICY IF EXISTS TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_ROW_POLICY;
-- DROP MASKING POLICY IF EXISTS TML_FLEET_TELEMETRY_HOL.GOVERNANCE.EMAIL_MASK;
-- DROP MASKING POLICY IF EXISTS TML_FLEET_TELEMETRY_HOL.GOVERNANCE.SERIAL_MASK;
-- DROP TABLE IF EXISTS TML_FLEET_TELEMETRY_HOL.GOVERNANCE.CUSTOMER_VEHICLES;
-- DROP SCHEMA IF EXISTS TML_FLEET_TELEMETRY_HOL.GOVERNANCE;


-- =============================================================
-- [14] DISCUSSION & LIVE Q&A
-- =============================================================
-- Take a moment to discuss what you've seen with the group.
-- Suggested questions:
--
--   ► How does the RAW → CURATED → BUSINESSREADY pipeline compare
--     to your current ETL architecture?
--
--   ► Which Cortex AI function (sentiment, summarize, classify,
--     translate, LLM complete) would add the most value to your
--     existing fleet operations?
--
--   ► How would the H3 geospatial road-segment join work with
--     your actual highway corridors and depot locations?
--
--   ► Does the per-customer RBAC + Row Access Policy model fit
--     your multi-tenant data isolation requirements?
--
--   ► What retention SLAs would you configure for raw vs curated
--     vs trip data in your production environment?
--
--   ► How could the real-time Interactive Table feed your
--     existing dashboards or alerting systems?
--
-- Ask your technical questions now — the floor is open!


/***********************************************************************
  END OF HANDS-ON LAB
  -------------------------------------------------------------------
  RECAP — What You Built & Explored:
  ┌──────────────────────────────────────────────────────────┐
  │  SHARED DB: TML_FLEET_TELEMETRY (read-only)             │
  │  ├─ RAW ingestion via Snowpipe Streaming (Kafka)        │
  │  ├─ CURATED signal-split views + late-arrival detection  │
  │  ├─ BUSINESSREADY trips, fleet health, 360 views        │
  │  ├─ GEOSPATIAL H3 road-segment fuel efficiency          │
  │  ├─ ROW ACCESS POLICY + MASKING POLICY (built in lab)   │
  │  ├─ CORTEX AI — text (sentiment, summarize, classify)    │
  │  ├─ CORTEX AI — multimodal image analysis (pixtral)     │
  │  ├─ CORTEX SEARCH — semantic search across fleet events  │
  │  └─ INTERACTIVE TABLE for real-time dashboards          │
  │                                                          │
  │  LAB DB: TML_FLEET_TELEMETRY_HOL (your sandbox)         │
  │  ├─ UDF: HAVERSINE_KM — GPS distance calculator         │
  │  ├─ UDF: ENGINE_HEALTH_GRADE — sensor-based grading     │
  │  ├─ VIEW: DRIVER_SCORECARD_V1 — A/B/C/D driver grades  │
  │  ├─ VIEW: SPEED_ALERT_REPORT_V1 — speed violations     │
  │  ├─ SP: GET_CUSTOMER_FLEET_REPORT — per-customer report │
  │  ├─ SP: GET_SPEED_VIOLATIONS — parameterized violations │
  │  ├─ RAP: CUSTOMER_ROW_POLICY — tenant row isolation     │
  │  ├─ MASK: EMAIL_MASK — PII masking on driver email      │
  │  ├─ MASK: SERIAL_MASK — PII masking on device serial    │
  │  └─ STAGE: IMAGE_STAGE — SSE stage for multimodal AI   │
  └──────────────────────────────────────────────────────────┘
***********************************************************************/
