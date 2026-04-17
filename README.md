# TML Fleet Telemetry V1 — Snowflake Demo

> **NewCo Datawarehouse Workshop Demo — Tata Motors (Commercial Vehicle Telemetry)**
>
> Production-grade, multi-tenant fleet telemetry pipeline using Snowpipe Streaming SDK,
> Dynamic Tables, Schema Evolution, Row Access Policies, and automated data retention.

## Architecture

```
Python Ingestion (Snowpipe Streaming SDK v1.2.0)
  5M vehicles · 100 customers · 2-phase schema evolution
       │
       ▼
RAW ──► CURATED Views (ARRIVED_LATE) ──► CURATED DTs (dedup) ──► BUSINESSREADY DTs/Views
                                                                       │
MASTER (geofences, road segments, retention policies) ─────────────────┘
TASK (daily) ──► PROC deletes expired rows ──► DT refresh propagates deletions
```

## Project Structure

```
tml-fleet-telemetry/
├── setup.sql                        # All SQL DDLs (Steps 1-11)
├── python/
│   ├── indian_routes.py             # Route definitions, geofences, geo-math
│   ├── kafka_event_generator_v1.py  # Event generator (48 Phase1 + 53 Phase2 fields)
│   ├── kafka_simulator_v1.py        # In-memory Kafka simulator
│   ├── snowpipe_consumer_v1.py      # Snowpipe Streaming SDK consumer pool
│   └── run_demo_v1.py               # Main orchestrator (two-phase ingestion)
├── data/
│   ├── road_segments.csv            # 236 Indian highway road segments
│   └── profile.json.template        # Connection config template
├── .gitignore
└── README.md
```

## Prerequisites

| Requirement | Details |
|---|---|
| Snowflake Account | Enterprise edition (for Row Access Policies) |
| Role | `ACCOUNTADMIN` for initial setup |
| Python | 3.9+ |
| Snowpipe Streaming SDK | `snowflake-ingest-streaming >= 1.2.0` |

## Step-by-Step Execution

### Step 1: Install Python Dependencies

```bash
pip install snowflake-ingest-streaming>=1.2.0
```

### Step 2: Generate RSA Key Pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### Step 3: Configure Connection Profile

Copy `data/profile.json.template` to `python/profile.json` and fill in:

```bash
cp data/profile.json.template python/profile.json
```

Edit `python/profile.json`:
- `account`: Your `<ORG>-<ACCOUNT_NAME>` (e.g., `SFSEAPAC-TML_HACKATHON_ACCOUNT`)
- `url`: `https://<ORG>-<ACCOUNT_NAME>.snowflakecomputing.com:443`
- `private_key_file`: Absolute path to `rsa_key.p8`

### Step 4: Execute SQL Setup (Steps 1-2 in setup.sql)

Open `setup.sql` in a Snowflake worksheet. Execute the following sections **in order**:

1. **STEP 1** — Creates warehouse `TML_HACKATHON`, database `TML_FLEET_TELEMETRY`, and 4 schemas (RAW, CURATED, BUSINESSREADY, MASTER)

2. **STEP 2** — Creates ingestion service user `TML_INGEST_USER`
   - **Important:** Replace `<RSA_PUBLIC_KEY_CONTENT>` with the content of `rsa_key.pub` (exclude the `BEGIN/END PUBLIC KEY` lines)

### Step 5: Load Reference Data (Step 3 in setup.sql)

Execute **STEP 3** in `setup.sql`:
- **3a.** Geofence definitions (25 Indian cities) — runs directly
- **3b.** Road segments — requires uploading `data/road_segments.csv`:
  ```sql
  PUT file:///path/to/data/road_segments.csv @TML_FLEET_TELEMETRY.MASTER.road_seg_stage;
  COPY INTO _road_seg_staging FROM @road_seg_stage FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1);
  ```
  Then run the `INSERT INTO ROAD_SEGMENTS` statement
- **3c.** Customer retention policies (100 customers) — runs directly

### Step 6: Create RAW Landing Table (Step 4 in setup.sql)

Execute **STEP 4** — Creates `VEHICLE_TELEMETRY_RAW_V1` with:
- 48 columns (5 more added via schema evolution in Phase 2)
- `ENABLE_SCHEMA_EVOLUTION = TRUE`
- `CHANGE_TRACKING = TRUE`
- Clustered by `(VEHICLE_ID, EVENT_TS)`

### Step 7: Run Python Ingestion

```bash
cd python/

python3 run_demo_v1.py \
    --profile ./profile.json \
    --total-vehicles 5000000 \
    --phase1-vehicles 2500000 \
    --phase1-iterations 1 \
    --phase1-partitions 10 \
    --phase2-iterations 4 \
    --phase2-partitions 5 \
    --batch-size 50000 \
    --consumer-batch 2000 \
    --time-step 600 \
    --drain-timeout 600
```

**What happens:**

| Phase | Vehicles | Columns | Description |
|---|---|---|---|
| Phase 1 | 2.5M (VH_0000000 – VH_2499999) | 48 (original) | Original schema, 10 channels |
| Phase 2 | 2.5M (VH_2500000 – VH_4999999) | 53 (+5 TYRE/DEF) | Schema evolution, 5 channels |

Expected: ~4.97M rows ingested in ~10-15 minutes.

### Step 8: Create CURATED Layer (Steps 5-6 in setup.sql)

Execute **STEP 5** (4 views with ARRIVED_LATE flag) and **STEP 6** (4 dynamic tables with ROW_NUMBER dedup, INCREMENTAL refresh, 1-min lag).

Wait for DT initialization to complete (~2-5 minutes for 5M rows).

### Step 9: Create BUSINESSREADY Layer (Steps 7-8 in setup.sql)

Execute **STEP 7** (6 dynamic tables) and **STEP 8** (3 views):
- `FACT_VEHICLE_DAILY_METRICS_V1` — Daily per-vehicle KPIs
- `FACT_CUSTOMER_FLEET_SUMMARY_V1` — Daily fleet-level KPIs
- `DIM_VEHICLE_V1` — Vehicle dimension
- `DIM_CUSTOMER_V1` — Customer dimension
- `FLEET_HEALTH_DASHBOARD_V1` — Vehicle health status
- `TRIP_SUMMARIES_V1` — Trip detection with geofence matching
- `VEHICLE360_V1` — Vehicle + daily metrics (view)
- `CUSTOMER360_V1` — Customer + fleet summary (view)
- `MONTHLY_ROAD_SEGMENT_FUEL_EFFICIENCY_V1` — H3-based road segment analytics (view)

### Step 10: Set Up Governance (Step 9 in setup.sql)

Execute **STEP 9**:
- Creates 100 customer roles (`CUST_0001_ROLE` through `CUST_0100_ROLE`)
- Creates Row Access Policy `TRIP_CUSTOMER_RAP`
- Applies RAP to `TRIP_SUMMARIES_V1`

### Step 11: Set Up Retention Enforcement (Step 10 in setup.sql)

Execute **STEP 10**:
- Creates stored procedure `ENFORCE_CUSTOMER_RETENTION`
- Creates scheduled task `CUSTOMER_RETENTION_TASK` (daily at 2 AM UTC)
- Uncomment `ALTER TASK ... RESUME` when ready to activate

### Step 12: Verify (Step 11 in setup.sql)

Execute **STEP 11** verification queries to confirm:
- RAW row count (~4.97M)
- All CURATED and BUSINESSREADY DTs are ACTIVE
- ARRIVED_LATE distribution (~43% TRUE / ~57% FALSE)
- Schema evolution columns present
- Retention proc executes successfully
- Row access policy isolates customer data

## Snowflake Features Demonstrated

1. **Snowpipe Streaming SDK** — Low-latency `append_row()` ingestion via RSA key-pair auth
2. **Schema Evolution** — Auto-adds 5 columns mid-stream (`ENABLE_SCHEMA_EVOLUTION=TRUE`)
3. **Dynamic Tables** — Declarative, continuously refreshing transformation pipeline
4. **INCREMENTAL Refresh** — Change-tracking-based DT refresh for append workloads
5. **Row Access Policy** — Per-customer tenant isolation without data duplication
6. **HAVERSINE + H3** — Built-in geospatial functions for geofence and road segment matching
7. **Scheduled Tasks** — Automated customer-specific data retention enforcement
8. **Clustering** — `CLUSTER BY (VEHICLE_ID, EVENT_TS)` for optimal pruning
9. **Change Tracking** — Enables INCREMENTAL DT refresh on RAW table
10. **Multi-layer DT DAG** — RAW -> CURATED -> BUSINESSREADY with automatic propagation

## Object Inventory (22 objects)

| Schema | Object | Type |
|---|---|---|
| RAW | VEHICLE_TELEMETRY_RAW_V1 | TABLE |
| RAW | ENFORCE_CUSTOMER_RETENTION | PROCEDURE |
| RAW | CUSTOMER_RETENTION_TASK | TASK |
| CURATED | VW_GPS_SIGNALS_V1 | VIEW |
| CURATED | VW_IMU_SIGNALS_V1 | VIEW |
| CURATED | VW_J1939_SIGNALS_V1 | VIEW |
| CURATED | VW_TELEMATICS_V1 | VIEW |
| CURATED | GPS_SIGNALS_V1 | DYNAMIC TABLE |
| CURATED | IMU_SIGNALS_V1 | DYNAMIC TABLE |
| CURATED | J1939_SIGNALS_V1 | DYNAMIC TABLE |
| CURATED | TELEMATICS_DEVICE_PARAMS_V1 | DYNAMIC TABLE |
| BUSINESSREADY | FACT_VEHICLE_DAILY_METRICS_V1 | DYNAMIC TABLE |
| BUSINESSREADY | FACT_CUSTOMER_FLEET_SUMMARY_V1 | DYNAMIC TABLE |
| BUSINESSREADY | DIM_VEHICLE_V1 | DYNAMIC TABLE |
| BUSINESSREADY | DIM_CUSTOMER_V1 | DYNAMIC TABLE |
| BUSINESSREADY | FLEET_HEALTH_DASHBOARD_V1 | DYNAMIC TABLE |
| BUSINESSREADY | TRIP_SUMMARIES_V1 | DYNAMIC TABLE |
| BUSINESSREADY | VEHICLE360_V1 | VIEW |
| BUSINESSREADY | CUSTOMER360_V1 | VIEW |
| BUSINESSREADY | MONTHLY_ROAD_SEGMENT_FUEL_EFFICIENCY_V1 | VIEW |
| BUSINESSREADY | TRIP_CUSTOMER_RAP | ROW ACCESS POLICY |
| MASTER | GEOFENCE_DEFINITIONS | TABLE |
| MASTER | ROAD_SEGMENTS | TABLE |
| MASTER | CUSTOMER_RETENTION_POLICIES | TABLE |
