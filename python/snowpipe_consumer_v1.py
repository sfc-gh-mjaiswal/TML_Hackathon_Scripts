import os
import time
import json
import uuid
import threading
from datetime import datetime, timezone

os.environ["SS_LOG_LEVEL"] = "warn"

from snowflake.ingest.streaming import StreamingIngestClient
from kafka_simulator_v1 import KafkaSimulatorV1
from kafka_event_generator_v1 import PHASE1_FIELDS, EVOLVED_FIELDS

DB_NAME = "TML_FLEET_TELEMETRY"
SCHEMA_NAME = "RAW"
TABLE_NAME = "VEHICLE_TELEMETRY_RAW_V1"
DLQ_TABLE = "DLQ_MESSAGES"

BACKPRESSURE_INITIAL_WAIT = 0.5
BACKPRESSURE_MAX_WAIT = 10.0
BACKPRESSURE_MAX_RETRIES = 10

ACTIVE_FIELDS = list(PHASE1_FIELDS)


def set_evolved_fields():
    global ACTIVE_FIELDS
    ACTIVE_FIELDS = list(EVOLVED_FIELDS)


def _flatten_event(event: dict) -> dict | None:
    try:
        content = event["record_content"]
        metadata = event["record_metadata"]
        row = {"RECORD_METADATA": json.dumps(metadata)}
        for field in ACTIVE_FIELDS:
            key = field.lower()
            if key in content:
                row[field] = content[key]
        return row
    except Exception:
        return None


class PartitionConsumer(threading.Thread):
    def __init__(
        self,
        kafka: KafkaSimulatorV1,
        partition_id: int,
        channel,
        dlq_channel,
        batch_size: int = 1000,
    ):
        super().__init__(daemon=True)
        self.kafka = kafka
        self.partition_id = partition_id
        self._channel = channel
        self._dlq_channel = dlq_channel
        self.batch_size = batch_size
        self._stop_event = threading.Event()
        self.rows_ingested = 0
        self.rows_dlq = 0
        self.backpressure_events = 0

    def _send_to_dlq(self, event: dict, error_msg: str):
        if self._dlq_channel is None:
            return
        try:
            dlq_row = {
                "RECORD_METADATA": json.dumps(event.get("record_metadata", {})),
                "RECORD_CONTENT": json.dumps(event.get("record_content", {})),
                "ERROR_MESSAGE": str(error_msg)[:1000],
                "SOURCE_TOPIC": "VEHICLE_TELEMETRY",
                "SOURCE_PARTITION": self.partition_id,
            }
            self._dlq_channel.append_row(dlq_row)
            self.rows_dlq += 1
        except Exception:
            pass

    def _ingest_row(self, row: dict) -> bool:
        wait = BACKPRESSURE_INITIAL_WAIT
        for attempt in range(BACKPRESSURE_MAX_RETRIES):
            try:
                self._channel.append_row(row)
                return True
            except Exception as e:
                err_str = str(e)
                if "ReceiverSaturated" in err_str or "429" in err_str:
                    self.backpressure_events += 1
                    if attempt < BACKPRESSURE_MAX_RETRIES - 1:
                        time.sleep(wait)
                        wait = min(wait * 2, BACKPRESSURE_MAX_WAIT)
                    else:
                        return False
                else:
                    raise
        return False

    def run(self):
        partition = self.kafka.get_partition(self.partition_id)
        while not self._stop_event.is_set():
            batch = partition.consume_batch(max_size=self.batch_size, timeout=0.1)
            if not batch:
                continue
            for event in batch:
                if self._stop_event.is_set():
                    break
                row = _flatten_event(event)
                if row is None:
                    self._send_to_dlq(event, "Failed to parse/flatten event")
                    continue
                try:
                    ok = self._ingest_row(row)
                    if ok:
                        self.rows_ingested += 1
                    else:
                        self._send_to_dlq(event, "Max backpressure retries exceeded")
                except Exception as e:
                    self._send_to_dlq(event, str(e))

    def stop(self):
        self._stop_event.set()


class SnowpipeConsumerPoolV1:
    def __init__(self, kafka: KafkaSimulatorV1, profile_path: str, batch_size: int = 1000):
        self.kafka = kafka
        self.profile_path = profile_path
        self.consumers: list[PartitionConsumer] = []
        self._clients = []
        self._channels = []

        uid = uuid.uuid4().hex[:6]

        print("    Opening DLQ channel...")
        dlq_client = StreamingIngestClient(
            client_name=f"tml_dlq_{uid}",
            db_name=DB_NAME, schema_name=SCHEMA_NAME,
            pipe_name=f"{DLQ_TABLE}-STREAMING",
            profile_json=profile_path,
        )
        dlq_channel, _ = dlq_client.open_channel(f"ch_dlq_{uid}")
        self._clients.append(dlq_client)
        self._channels.append(dlq_channel)
        print("    DLQ channel opened")

        pipe_name = f"{TABLE_NAME}-STREAMING"
        print(f"    Opening client for {TABLE_NAME}...")
        client = StreamingIngestClient(
            client_name=f"tml_telemetry_{uid}",
            db_name=DB_NAME, schema_name=SCHEMA_NAME,
            pipe_name=pipe_name,
            profile_json=profile_path,
        )
        self._clients.append(client)

        for p in range(kafka.num_partitions):
            ch_name = f"ch_telemetry_p{p}_{uid}"
            channel, status = client.open_channel(ch_name)
            self._channels.append(channel)

            consumer = PartitionConsumer(
                kafka=kafka,
                partition_id=p,
                channel=channel,
                dlq_channel=dlq_channel,
                batch_size=batch_size,
            )
            self.consumers.append(consumer)
        print(f"    {TABLE_NAME}: {kafka.num_partitions} channels opened")

    def start_all(self):
        for c in self.consumers:
            c.start()

    def stop_all(self):
        for c in self.consumers:
            c.stop()

    def close_all(self):
        for c in self.consumers:
            c.stop()
        time.sleep(1)
        for ch in self._channels:
            try:
                ch.close()
            except Exception:
                pass
        for cl in self._clients:
            try:
                cl.close()
            except Exception:
                pass

    def total_ingested(self) -> int:
        return sum(c.rows_ingested for c in self.consumers)

    def total_dlq(self) -> int:
        return sum(c.rows_dlq for c in self.consumers)

    def total_backpressure(self) -> int:
        return sum(c.backpressure_events for c in self.consumers)
