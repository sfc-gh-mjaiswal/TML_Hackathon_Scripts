import time
import signal
import argparse
import threading
from datetime import datetime, timezone

from kafka_event_generator_v1 import (
    init_vehicles, generate_flat_event, generate_flat_event_evolved,
    TOPIC, _partition_for_vehicle,
)
from kafka_simulator_v1 import KafkaSimulatorV1
from snowpipe_consumer_v1 import SnowpipeConsumerPoolV1, set_evolved_fields

shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    print("\nShutdown requested. Draining queues...")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def producer_loop(
    kafka, vehicles, batch_size, num_partitions, dt_sec=1.0, use_evolved=False, phase_label="",
):
    global shutdown_requested
    total_produced = 0
    offsets = {p: 0 for p in range(num_partitions)}
    start_time = time.time()

    gen_fn = generate_flat_event_evolved if use_evolved else generate_flat_event

    for offset in range(0, len(vehicles), batch_size):
        if shutdown_requested:
            break
        chunk = vehicles[offset : offset + batch_size]
        for v in chunk:
            if shutdown_requested:
                break
            vid = v["vehicle_id"]
            pid = _partition_for_vehicle(vid, num_partitions)
            offsets[pid] += 1
            event = gen_fn(v, offsets[pid], dt_sec=dt_sec)
            kafka.produce(vid, event)
            total_produced += 1

        elapsed = time.time() - start_time
        eps = total_produced / elapsed if elapsed > 0 else 0
        print(
            f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {phase_label} "
            f"Produced: {total_produced:,}/{len(vehicles):,} | "
            f"Rate: {eps:,.0f} events/sec | "
            f"Queued: {kafka.total_queued():,}"
        )

    return total_produced


def drain_and_shutdown(kafka, pool, monitor_stop, drain_timeout, label=""):
    print(f"\n  [{label}] Waiting for consumers to drain (timeout={drain_timeout}s)...")
    drain_start = time.time()
    last_ingested = -1
    stall_start = None
    while kafka.total_queued() > 0 and (time.time() - drain_start) < drain_timeout:
        if shutdown_requested:
            break
        time.sleep(1)
        current = pool.total_ingested()
        queued = kafka.total_queued()
        print(f"    Queued: {queued:,} | Ingested: {current:,}")
        if current == last_ingested:
            if stall_start is None:
                stall_start = time.time()
            elif time.time() - stall_start > 60:
                print(f"    WARNING: Ingestion stalled for 60s, breaking drain loop")
                break
        else:
            stall_start = None
        last_ingested = current

    monitor_stop.set()
    pool.stop_all()
    time.sleep(2)
    pool.close_all()
    remaining = kafka.total_queued()
    if remaining > 0:
        print(f"    WARNING: {remaining:,} events still queued after drain timeout")
    else:
        print(f"    All queued events drained successfully")
    return pool.total_ingested(), pool.total_dlq(), pool.total_backpressure()


def run_phase(
    profile_path, consumer_batch, batch_size, time_step, drain_timeout,
    num_partitions, iterations, vehicles_per_iter, start_idx,
    use_evolved, phase_name, step_offset, total_steps,
):
    global shutdown_requested

    kafka = KafkaSimulatorV1(num_partitions=num_partitions)

    print(f"\n  Starting Snowpipe Streaming consumer pool ({num_partitions} channels)...")
    pool = SnowpipeConsumerPoolV1(
        kafka=kafka,
        profile_path=profile_path,
        batch_size=consumer_batch,
    )
    pool.start_all()
    print(f"    {len(pool.consumers)} consumers started")

    phase_start = time.time()
    monitor_stop = threading.Event()

    def monitor():
        while not monitor_stop.is_set():
            time.sleep(5)
            ingested = pool.total_ingested()
            dlq = pool.total_dlq()
            bp = pool.total_backpressure()
            elapsed = time.time() - phase_start
            ips = ingested / elapsed if elapsed > 0 else 0
            print(
                f"  [CONSUMER {datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                f"Ingested: {ingested:,} ({ips:,.0f}/s) | "
                f"DLQ: {dlq} | BP: {bp} | "
                f"Queued: {kafka.total_queued():,}"
            )

    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()

    total_produced = 0
    current_idx = start_idx

    try:
        for iteration in range(1, iterations + 1):
            if shutdown_requested:
                break

            vid_start = current_idx
            vid_end = current_idx + vehicles_per_iter - 1
            col_count = 53 if use_evolved else 48
            step_num = step_offset + iteration

            print(f"\n  [{step_num}/{total_steps}] {phase_name} - Iteration {iteration}/{iterations}: "
                  f"{vehicles_per_iter:,} vehicles | {'EVOLVED' if use_evolved else 'ORIGINAL'} schema ({col_count} columns)")
            print(f"    Vehicle IDs: VH_{vid_start:07d} - VH_{vid_end:07d}")
            print("  " + "-" * 76)

            vehicles = init_vehicles(vehicles_per_iter, start_idx=current_idx)
            print(f"    {len(vehicles):,} vehicles initialized")

            produced = producer_loop(
                kafka, vehicles, batch_size, num_partitions,
                dt_sec=time_step, use_evolved=use_evolved,
                phase_label=f"[{phase_name} Iter {iteration}]",
            )
            total_produced += produced
            current_idx += vehicles_per_iter
            del vehicles
            print(f"    Iteration {iteration} complete: {produced:,} events produced")

    finally:
        ingested, dlq, bp = drain_and_shutdown(kafka, pool, monitor_stop, drain_timeout, phase_name)

    phase_elapsed = time.time() - phase_start
    return {
        "produced": total_produced,
        "ingested": ingested,
        "dlq": dlq,
        "bp": bp,
        "elapsed": phase_elapsed,
        "end_idx": current_idx,
    }


def main():
    parser = argparse.ArgumentParser(
        description="TML Fleet Telemetry V1 - Single Topic | 5M Vehicles | Schema Evolution"
    )
    parser.add_argument("--profile", default="profile.json")
    parser.add_argument("--total-vehicles", type=int, default=5_000_000)
    parser.add_argument("--phase1-vehicles", type=int, default=2_500_000,
                        help="Vehicles in Phase 1 (original schema)")
    parser.add_argument("--phase1-iterations", type=int, default=1,
                        help="Number of iterations for Phase 1 (default: 1)")
    parser.add_argument("--phase1-partitions", type=int, default=10,
                        help="Kafka partitions / Snowpipe channels for Phase 1 (default: 10)")
    parser.add_argument("--phase2-iterations", type=int, default=4,
                        help="Number of iterations for Phase 2 (default: 4)")
    parser.add_argument("--phase2-partitions", type=int, default=5,
                        help="Kafka partitions / Snowpipe channels for Phase 2 (default: 5)")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--consumer-batch", type=int, default=2000)
    parser.add_argument("--time-step", type=int, default=600)
    parser.add_argument("--drain-timeout", type=int, default=600,
                        help="Max seconds to wait for queue drain after each phase (default: 600)")
    args = parser.parse_args()

    phase2_vehicles = args.total_vehicles - args.phase1_vehicles
    p1_per_iter = args.phase1_vehicles // args.phase1_iterations
    p2_per_iter = phase2_vehicles // args.phase2_iterations
    total_steps = args.phase1_iterations + args.phase2_iterations

    print("=" * 80)
    print("TML FLEET TELEMETRY V1 - Single Topic | Flat Columns | Schema Evolution")
    print("=" * 80)
    print(f"  Total vehicles:      {args.total_vehicles:,}")
    print(f"  Customers:           100")
    print(f"  Topic:               {TOPIC}")
    print(f"  Drain timeout:       {args.drain_timeout}s")
    print()
    print(f"  PHASE 1 (Original Schema — 48 columns):")
    print(f"    Vehicles:          {args.phase1_vehicles:,}")
    print(f"    Iterations:        {args.phase1_iterations}")
    print(f"    Vehicles/iter:     {p1_per_iter:,}")
    print(f"    Partitions:        {args.phase1_partitions}")
    print(f"    Vehicle IDs:       VH_0000000 - VH_{args.phase1_vehicles - 1:07d}")
    print()
    print(f"  PHASE 2 (Evolved Schema — 53 columns, +5 TYRE/DEF):")
    print(f"    Vehicles:          {phase2_vehicles:,}")
    print(f"    Iterations:        {args.phase2_iterations}")
    print(f"    Vehicles/iter:     {p2_per_iter:,}")
    print(f"    Partitions:        {args.phase2_partitions}")
    print(f"    Vehicle IDs:       VH_{args.phase1_vehicles:07d} - VH_{args.total_vehicles - 1:07d}")
    print()
    print(f"  Total events:        {args.total_vehicles:,}")
    print(f"  Target table:        TML_FLEET_TELEMETRY.RAW.VEHICLE_TELEMETRY_RAW_V1")
    print(f"  Schema evolution:    ENABLE_SCHEMA_EVOLUTION=TRUE (auto-adds columns)")
    print("=" * 80)

    overall_start = time.time()

    print("\n" + "=" * 80)
    print("PHASE 1: ORIGINAL SCHEMA (48 columns)")
    print("=" * 80)

    p1_result = run_phase(
        profile_path=args.profile,
        consumer_batch=args.consumer_batch,
        batch_size=args.batch_size,
        time_step=args.time_step,
        drain_timeout=args.drain_timeout,
        num_partitions=args.phase1_partitions,
        iterations=args.phase1_iterations,
        vehicles_per_iter=p1_per_iter,
        start_idx=0,
        use_evolved=False,
        phase_name="P1",
        step_offset=0,
        total_steps=total_steps,
    )

    print(f"\n  PHASE 1 RESULT: Produced={p1_result['produced']:,} | "
          f"Ingested={p1_result['ingested']:,} | DLQ={p1_result['dlq']} | "
          f"Time={p1_result['elapsed']:.1f}s")

    print("\n" + "=" * 80)
    print("SCHEMA EVOLUTION: Adding 5 new J1939 columns via Snowpipe Streaming SDK")
    print("  + J1939_TYRE_PRESSURE_FRONT_LEFT_KPA  (FLOAT)")
    print("  + J1939_TYRE_PRESSURE_FRONT_RIGHT_KPA (FLOAT)")
    print("  + J1939_TYRE_PRESSURE_REAR_LEFT_KPA   (FLOAT)")
    print("  + J1939_TYRE_PRESSURE_REAR_RIGHT_KPA  (FLOAT)")
    print("  + J1939_DEF_LEVEL_PCT                 (FLOAT)")
    print("  Reducing channels from {} to {} for evolved schema".format(
        args.phase1_partitions, args.phase2_partitions))
    print("=" * 80)

    set_evolved_fields()
    print("  Consumer FIELD_MAP updated — now sending 53 columns (was 48)")

    print("\n" + "=" * 80)
    print("PHASE 2: EVOLVED SCHEMA (53 columns)")
    print("=" * 80)

    p2_result = run_phase(
        profile_path=args.profile,
        consumer_batch=args.consumer_batch,
        batch_size=args.batch_size,
        time_step=args.time_step,
        drain_timeout=args.drain_timeout,
        num_partitions=args.phase2_partitions,
        iterations=args.phase2_iterations,
        vehicles_per_iter=p2_per_iter,
        start_idx=args.phase1_vehicles,
        use_evolved=True,
        phase_name="P2",
        step_offset=args.phase1_iterations,
        total_steps=total_steps,
    )

    print(f"\n  PHASE 2 RESULT: Produced={p2_result['produced']:,} | "
          f"Ingested={p2_result['ingested']:,} | DLQ={p2_result['dlq']} | "
          f"Time={p2_result['elapsed']:.1f}s")

    overall_elapsed = time.time() - overall_start
    total_produced = p1_result["produced"] + p2_result["produced"]
    total_ingested = p1_result["ingested"] + p2_result["ingested"]
    total_dlq = p1_result["dlq"] + p2_result["dlq"]
    total_bp = p1_result["bp"] + p2_result["bp"]

    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"  Total vehicles:        {args.total_vehicles:,}")
    print(f"  Phase 1 (original):    {p1_result['ingested']:,} / {p1_result['produced']:,} ingested "
          f"({p1_result['ingested']/max(1,p1_result['produced'])*100:.1f}%) | "
          f"{args.phase1_partitions} channels | {p1_result['elapsed']:.1f}s")
    print(f"  Phase 2 (evolved):     {p2_result['ingested']:,} / {p2_result['produced']:,} ingested "
          f"({p2_result['ingested']/max(1,p2_result['produced'])*100:.1f}%) | "
          f"{args.phase2_partitions} channels | {p2_result['elapsed']:.1f}s")
    print(f"  Total produced:        {total_produced:,}")
    print(f"  Total ingested:        {total_ingested:,}")
    print(f"  Total DLQ:             {total_dlq:,}")
    print(f"  Backpressure events:   {total_bp:,}")
    print(f"  Elapsed time:          {overall_elapsed:.1f}s ({overall_elapsed/60:.1f} min)")
    print(f"  Produce rate:          {total_produced / overall_elapsed:,.0f} events/sec")
    print(f"  Ingest rate:           {total_ingested / overall_elapsed:,.0f} events/sec")
    print()
    p1_channels_time = args.phase1_partitions * p1_result["elapsed"] / 60
    p2_channels_time = args.phase2_partitions * p2_result["elapsed"] / 60
    ss_credits = (p1_channels_time + p2_channels_time) * 0.000038
    wh_credits = (overall_elapsed / 3600) * 8
    print(f"  Snowpipe Streaming throughput: {total_ingested / overall_elapsed:,.0f} events/sec")
    print(f"  Est. Snowpipe Streaming credits: ~{ss_credits:.2f}")
    print(f"  Est. Warehouse credits (Large, DT refresh): ~{wh_credits:.2f}")
    print(f"  Est. Total credits: ~{ss_credits + wh_credits:.2f}")
    print("=" * 80)


if __name__ == "__main__":
    main()
