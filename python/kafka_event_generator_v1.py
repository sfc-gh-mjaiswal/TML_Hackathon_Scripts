import random
import hashlib
from datetime import datetime, timezone

from indian_routes import (
    INDIAN_ROUTES, GEOFENCE_CITIES, ROAD_SEGMENTS,
    interpolate_waypoints, bearing_between, haversine_km, is_in_geofence,
)

NUM_CUSTOMERS = 100
NUM_VEHICLES = 5_000_000
CUSTOMER_IDS = [f"CUST_{i:04d}" for i in range(1, NUM_CUSTOMERS + 1)]
VEHICLES_PER_CUSTOMER = NUM_VEHICLES // NUM_CUSTOMERS

TOPIC = "VEHICLE_TELEMETRY"
NUM_PARTITIONS = 10

SPEED_PROFILES = {
    "highway": {"cruise_min": 60, "cruise_max": 90, "accel": 2.5, "decel": -3.0},
    "city_exit": {"cruise_min": 20, "cruise_max": 45, "accel": 1.5, "decel": -2.0},
    "city_enter": {"cruise_min": 15, "cruise_max": 40, "accel": 1.5, "decel": -2.5},
}

PHASE1_FIELDS = [
    "EVENT_TS", "CUSTOMER_ID", "VEHICLE_ID", "DEVICE_ID",
    "GPS_LATITUDE", "GPS_LONGITUDE", "GPS_ALTITUDE_M", "GPS_SPEED_KPH",
    "GPS_HEADING_DEG", "GPS_HDOP", "GPS_SATELLITE_COUNT", "GPS_FIX_QUALITY",
    "IMU_ACCEL_X_G", "IMU_ACCEL_Y_G", "IMU_ACCEL_Z_G",
    "IMU_GYRO_X_DPS", "IMU_GYRO_Y_DPS", "IMU_GYRO_Z_DPS",
    "J1939_VEHICLE_SPEED_KPH", "J1939_ENGINE_RPM", "J1939_ACCELERATOR_PEDAL_PCT",
    "J1939_ENGINE_LOAD_PCT", "J1939_ENGINE_TORQUE_PCT", "J1939_FUEL_RATE_LPH",
    "J1939_TOTAL_FUEL_USED_L", "J1939_FUEL_LEVEL_PCT", "J1939_COOLANT_TEMP_C",
    "J1939_OIL_TEMP_C", "J1939_OIL_PRESSURE_KPA", "J1939_INTAKE_MANIFOLD_PRESSURE_KPA",
    "J1939_BATTERY_VOLTAGE_V", "J1939_ENGINE_HOURS", "J1939_ODOMETER_KM",
    "J1939_GEAR_SELECTED", "J1939_BRAKE_SWITCH", "J1939_CLUTCH_SWITCH",
    "J1939_PTO_STATUS", "J1939_ACTIVE_DTC_COUNT",
    "TEL_IGNITION_STATUS", "TEL_EXTERNAL_POWER_V", "TEL_BACKUP_BATTERY_V",
    "TEL_DEVICE_TEMP_C", "TEL_GSM_SIGNAL_DBM", "TEL_NETWORK_TYPE",
    "TEL_GNSS_FIX_STATUS", "TEL_CAN_BUS_HEALTH", "TEL_STORAGE_QUEUE_DEPTH",
    "TEL_TAMPER_ALERT",
]

EVOLVED_FIELDS = PHASE1_FIELDS + [
    "J1939_TYRE_PRESSURE_FRONT_LEFT_KPA",
    "J1939_TYRE_PRESSURE_FRONT_RIGHT_KPA",
    "J1939_TYRE_PRESSURE_REAR_LEFT_KPA",
    "J1939_TYRE_PRESSURE_REAR_RIGHT_KPA",
    "J1939_DEF_LEVEL_PCT",
]


def _partition_for_vehicle(vehicle_id: str, num_partitions: int = None) -> int:
    if num_partitions is None:
        num_partitions = NUM_PARTITIONS
    return int(hashlib.md5(vehicle_id.encode()).hexdigest(), 16) % num_partitions


def _assign_route(vehicle_idx: int) -> dict:
    route = INDIAN_ROUTES[vehicle_idx % len(INDIAN_ROUTES)]
    direction = 1 if (vehicle_idx // len(INDIAN_ROUTES)) % 2 == 0 else -1
    wps = route["waypoints"] if direction == 1 else list(reversed(route["waypoints"]))
    bucket = vehicle_idx % 10
    if bucket == 0:
        progress = random.uniform(0.998, 0.9999)
    elif bucket == 1:
        progress = random.uniform(0.98, 0.997)
    elif bucket <= 3:
        progress = random.uniform(0.7, 0.97)
    else:
        progress = random.uniform(0, 0.7)
    total_wps = len(wps)
    seg_float = progress * (total_wps - 1)
    seg_idx = min(int(seg_float), total_wps - 2)
    seg_frac = seg_float - seg_idx
    lat, lon = interpolate_waypoints(wps[seg_idx], wps[seg_idx + 1], seg_frac)
    heading = bearing_between(wps[seg_idx], wps[seg_idx + 1])
    from_city = route["from"] if direction == 1 else route["to"]
    to_city = route["to"] if direction == 1 else route["from"]
    return {
        "route_name": route["name"],
        "highway": route["highway"],
        "from_city": from_city,
        "to_city": to_city,
        "distance_km": route["distance_km"],
        "waypoints": wps,
        "seg_idx": seg_idx,
        "seg_frac": seg_frac,
        "direction": direction,
        "lat": lat,
        "lon": lon,
        "heading": heading,
        "trip_active": True,
        "trip_number": 1,
        "trip_start_odo": None,
        "trip_start_fuel": None,
        "idle_ticks": 0,
        "harsh_accel_count": 0,
        "harsh_brake_count": 0,
    }


def _advance_along_route(v: dict, dt_sec: float = 1.0):
    route_info = v["_route"]
    wps = route_info["waypoints"]
    speed_kph = v["speed"]
    seg_idx = route_info["seg_idx"]
    seg_frac = route_info["seg_frac"]

    if not route_info["trip_active"]:
        v["speed"] = 0
        route_info["idle_ticks"] += 1
        idle_threshold = max(2, int((30 + random.randint(0, 60)) / max(dt_sec, 1)))
        if route_info["idle_ticks"] > idle_threshold:
            _start_new_trip(v)
        return

    total_wps = len(wps)
    wp1 = wps[seg_idx]
    wp2 = wps[seg_idx + 1] if seg_idx + 1 < total_wps else wps[seg_idx]
    seg_dist_km = haversine_km(wp1[0], wp1[1], wp2[0], wp2[1])

    progress_ratio = (seg_idx + seg_frac) / max(total_wps - 1, 1)
    if progress_ratio < 0.08:
        profile = SPEED_PROFILES["city_exit"]
    elif progress_ratio > 0.92:
        profile = SPEED_PROFILES["city_enter"]
    else:
        profile = SPEED_PROFILES["highway"]

    target_speed = random.uniform(profile["cruise_min"], profile["cruise_max"])
    diff = target_speed - speed_kph
    if diff > 0:
        accel = min(diff, profile["accel"] + random.uniform(-0.5, 0.5))
        speed_kph += accel
        if accel > 3.5:
            route_info["harsh_accel_count"] += 1
    else:
        decel = max(diff, profile["decel"] + random.uniform(-0.5, 0.5))
        speed_kph += decel
        if decel < -4.0:
            route_info["harsh_brake_count"] += 1

    speed_kph = max(0, min(speed_kph, 120))
    v["speed"] = speed_kph

    dist_this_tick = (speed_kph / 3600) * dt_sec
    if seg_dist_km > 0.001:
        frac_advance = dist_this_tick / seg_dist_km
    else:
        frac_advance = 1.0

    seg_frac += frac_advance

    while seg_frac >= 1.0 and seg_idx < total_wps - 2:
        seg_frac -= 1.0
        seg_idx += 1
        if seg_idx < total_wps - 1:
            wp1 = wps[seg_idx]
            wp2 = wps[seg_idx + 1]
            seg_dist_km = haversine_km(wp1[0], wp1[1], wp2[0], wp2[1])

    cur_lat, cur_lon = interpolate_waypoints(wps[seg_idx], wps[min(seg_idx + 1, total_wps - 1)], min(seg_frac, 1.0))
    dist_to_end = haversine_km(cur_lat, cur_lon, wps[-1][0], wps[-1][1])

    if dist_to_end < 0.5 or (seg_idx >= total_wps - 2 and seg_frac >= 1.0):
        seg_idx = total_wps - 2
        seg_frac = 1.0
        final_lat, final_lon = wps[-1]
        v["lat"] = final_lat
        v["lon"] = final_lon
        route_info["trip_active"] = False
        v["speed"] = 0
        route_info["idle_ticks"] = 0
        v["ignition"] = "OFF"
        route_info["seg_idx"] = seg_idx
        route_info["seg_frac"] = seg_frac
        return

    route_info["seg_idx"] = seg_idx
    route_info["seg_frac"] = seg_frac

    wp1 = wps[seg_idx]
    wp2 = wps[min(seg_idx + 1, total_wps - 1)]
    lat, lon = interpolate_waypoints(wp1, wp2, min(seg_frac, 1.0))
    lat += random.gauss(0, 0.0003)
    lon += random.gauss(0, 0.0003)
    v["lat"] = lat
    v["lon"] = lon
    route_info["heading"] = bearing_between(wp1, wp2) + random.gauss(0, 2)


def _start_new_trip(v: dict):
    route_info = v["_route"]
    route_info["trip_number"] += 1
    old_to = route_info["to_city"]
    old_from = route_info["from_city"]
    old_wps = route_info["waypoints"]
    route_info["waypoints"] = list(reversed(old_wps))
    route_info["from_city"] = old_to
    route_info["to_city"] = old_from
    route_info["seg_idx"] = 0
    route_info["seg_frac"] = 0.0
    route_info["trip_active"] = True
    route_info["idle_ticks"] = 0
    route_info["harsh_accel_count"] = 0
    route_info["harsh_brake_count"] = 0
    route_info["trip_start_odo"] = v["odometer"]
    route_info["trip_start_fuel"] = v["total_fuel"]
    v["ignition"] = "ON"
    v["speed"] = 0


def init_vehicles(num_vehicles: int = NUM_VEHICLES, start_idx: int = 0) -> list[dict]:
    vehicles = []
    veh_per_cust = num_vehicles // NUM_CUSTOMERS
    for cust_idx, cust_id in enumerate(CUSTOMER_IDS):
        for vi in range(veh_per_cust):
            global_idx = start_idx + cust_idx * veh_per_cust + vi
            if len(vehicles) >= num_vehicles:
                break
            vid = f"VH_{global_idx:07d}"
            did = f"DEV_{global_idx:07d}"
            route = _assign_route(global_idx)
            v = {
                "customer_id": cust_id,
                "vehicle_id": vid,
                "device_id": did,
                "lat": route["lat"],
                "lon": route["lon"],
                "speed": random.uniform(40, 80),
                "heading": route["heading"],
                "odometer": random.uniform(10000, 300000),
                "total_fuel": random.uniform(5000, 50000),
                "engine_hours": random.uniform(500, 20000),
                "fuel_level": random.uniform(20, 95),
                "coolant_temp": random.uniform(75, 95),
                "oil_temp": random.uniform(85, 105),
                "ignition": "ON",
                "_route": route,
            }
            route["trip_start_odo"] = v["odometer"]
            route["trip_start_fuel"] = v["total_fuel"]
            vehicles.append(v)
    return vehicles[:num_vehicles]


def _make_record_metadata(vehicle_id: str, offset: int) -> dict:
    partition = _partition_for_vehicle(vehicle_id)
    return {
        "offset": offset,
        "topic": TOPIC,
        "partition": partition,
        "key": vehicle_id,
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "SnowflakeConnectorPushTime": int(datetime.now(timezone.utc).timestamp() * 1000),
        "headers": {},
    }


def generate_flat_event(vehicle: dict, offset: int, dt_sec: float = 1.0) -> dict:
    _advance_along_route(vehicle, dt_sec=dt_sec)
    route_info = vehicle["_route"]
    speed = vehicle["speed"]
    ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    vehicle["odometer"] += speed / 3600 * dt_sec
    load_factor = random.uniform(0.3, 1.0)
    if speed < 5:
        fuel_rate = 2.0 + random.uniform(0, 1.0)
    elif speed < 40:
        fuel_rate = 8.0 + (speed / 40) * 8 * load_factor + random.uniform(-1, 1)
    elif speed < 80:
        fuel_rate = 12.0 + (speed / 80) * 10 * load_factor + random.uniform(-2, 2)
    else:
        fuel_rate = 18.0 + (speed / 100) * 15 * load_factor + random.uniform(-3, 3)
    fuel_rate = max(1.0, fuel_rate)
    vehicle["total_fuel"] += fuel_rate / 3600 * dt_sec
    vehicle["fuel_level"] = max(5, vehicle["fuel_level"] - fuel_rate * 0.0001 * dt_sec)
    vehicle["engine_hours"] += dt_sec / 3600
    vehicle["coolant_temp"] = 80 + (speed / 100) * 10 + random.uniform(-2, 3)
    vehicle["oil_temp"] = 90 + (speed / 100) * 12 + random.uniform(-2, 3)
    engine_load = 20 + (speed / 100) * 50 * load_factor + random.uniform(-5, 5)

    if route_info["trip_active"] and vehicle["ignition"] == "OFF":
        vehicle["ignition"] = "ON"

    record_metadata = _make_record_metadata(vehicle["vehicle_id"], offset)

    row = {
        "event_ts": ts_str,
        "customer_id": vehicle["customer_id"],
        "vehicle_id": vehicle["vehicle_id"],
        "device_id": vehicle["device_id"],
        "gps_latitude": round(vehicle["lat"], 6),
        "gps_longitude": round(vehicle["lon"], 6),
        "gps_altitude_m": round(500 + random.uniform(-50, 50), 1),
        "gps_speed_kph": round(speed, 2),
        "gps_heading_deg": round(route_info["heading"] % 360, 2),
        "gps_hdop": round(random.uniform(0.5, 3.0), 2),
        "gps_satellite_count": random.randint(6, 14),
        "gps_fix_quality": random.choice([1, 2, 4, 5]),
        "imu_accel_x_g": round(random.gauss(0.02 * (speed / 80), 0.12), 4),
        "imu_accel_y_g": round(random.gauss(0, 0.08), 4),
        "imu_accel_z_g": round(random.gauss(1.0, 0.05), 4),
        "imu_gyro_x_dps": round(random.gauss(0, 2), 4),
        "imu_gyro_y_dps": round(random.gauss(0, 2), 4),
        "imu_gyro_z_dps": round(random.gauss(0, 1.5), 4),
        "j1939_vehicle_speed_kph": round(speed, 2),
        "j1939_engine_rpm": round(800 + (speed / 110) * 1800 + random.uniform(-80, 80), 1),
        "j1939_accelerator_pedal_pct": round(min(95, max(0, speed / 100 * 70 + random.uniform(-10, 10))), 1),
        "j1939_engine_load_pct": round(min(95, max(5, engine_load)), 1),
        "j1939_engine_torque_pct": round(min(90, max(5, engine_load * 0.9 + random.uniform(-5, 5))), 1),
        "j1939_fuel_rate_lph": round(fuel_rate, 2),
        "j1939_total_fuel_used_l": round(vehicle["total_fuel"], 2),
        "j1939_fuel_level_pct": round(vehicle["fuel_level"], 1),
        "j1939_coolant_temp_c": round(vehicle["coolant_temp"], 1),
        "j1939_oil_temp_c": round(vehicle["oil_temp"], 1),
        "j1939_oil_pressure_kpa": round(random.uniform(200, 500), 1),
        "j1939_intake_manifold_pressure_kpa": round(80 + (engine_load / 100) * 170 + random.uniform(-10, 10), 1),
        "j1939_battery_voltage_v": round(random.uniform(24, 27.5), 2),
        "j1939_engine_hours": round(vehicle["engine_hours"], 2),
        "j1939_odometer_km": round(vehicle["odometer"], 2),
        "j1939_gear_selected": max(1, min(6, int(speed / 20) + 1)),
        "j1939_brake_switch": random.random() < (0.15 if speed < 30 else 0.03),
        "j1939_clutch_switch": random.random() < 0.02,
        "j1939_pto_status": random.random() < 0.01,
        "j1939_active_dtc_count": random.choices([0, 0, 0, 1], k=1)[0],
        "tel_ignition_status": vehicle["ignition"],
        "tel_external_power_v": round(random.uniform(12, 14), 2),
        "tel_backup_battery_v": round(random.uniform(3.7, 4.1), 2),
        "tel_device_temp_c": round(random.uniform(30, 55), 1),
        "tel_gsm_signal_dbm": round(random.uniform(-95, -60), 1),
        "tel_network_type": random.choice(["4G", "4G", "3G", "5G"]),
        "tel_gnss_fix_status": "3D" if speed > 0 else "2D",
        "tel_can_bus_health": random.choice(["OK", "OK", "OK", "DEGRADED"]),
        "tel_storage_queue_depth": random.randint(0, 10),
        "tel_tamper_alert": False,
    }

    return {
        "record_metadata": record_metadata,
        "record_content": row,
    }


def generate_flat_event_evolved(vehicle: dict, offset: int, dt_sec: float = 1.0) -> dict:
    event = generate_flat_event(vehicle, offset, dt_sec=dt_sec)
    speed = vehicle["speed"]
    event["record_content"]["j1939_tyre_pressure_front_left_kpa"] = round(random.uniform(720, 830) - speed * 0.05, 1)
    event["record_content"]["j1939_tyre_pressure_front_right_kpa"] = round(random.uniform(720, 830) - speed * 0.05, 1)
    event["record_content"]["j1939_tyre_pressure_rear_left_kpa"] = round(random.uniform(700, 810) - speed * 0.04, 1)
    event["record_content"]["j1939_tyre_pressure_rear_right_kpa"] = round(random.uniform(700, 810) - speed * 0.04, 1)
    event["record_content"]["j1939_def_level_pct"] = round(max(5, 85 - vehicle.get("engine_hours", 0) * 0.3 + random.uniform(-3, 3)), 1)
    return event
