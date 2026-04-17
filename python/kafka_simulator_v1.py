import threading
import queue
from collections import defaultdict

from kafka_event_generator_v1 import _partition_for_vehicle, TOPIC


class KafkaPartition:
    def __init__(self, topic: str, partition_id: int):
        self.topic = topic
        self.partition_id = partition_id
        self._queue = queue.Queue(maxsize=500_000)
        self._offset = 0
        self._lock = threading.Lock()

    def produce(self, event: dict):
        with self._lock:
            self._offset += 1
            event["record_metadata"]["offset"] = self._offset
        self._queue.put(event, timeout=30)

    def consume(self, timeout: float = 0.1) -> dict | None:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def consume_batch(self, max_size: int = 1000, timeout: float = 0.1) -> list[dict]:
        batch = []
        for _ in range(max_size):
            event = self.consume(timeout=timeout if not batch else 0.01)
            if event is None:
                break
            batch.append(event)
        return batch

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def offset(self) -> int:
        return self._offset


class KafkaSimulatorV1:
    def __init__(self, num_partitions: int):
        self.topic = TOPIC
        self.num_partitions = num_partitions
        self.partitions: list[KafkaPartition] = [
            KafkaPartition(TOPIC, p) for p in range(num_partitions)
        ]

    def produce(self, vehicle_id: str, event: dict):
        partition_id = _partition_for_vehicle(vehicle_id, self.num_partitions)
        self.partitions[partition_id].produce(event)

    def get_partition(self, partition_id: int) -> KafkaPartition:
        return self.partitions[partition_id]

    def stats(self) -> dict:
        result = {}
        for p in range(self.num_partitions):
            part = self.partitions[p]
            result[f"p{p}"] = {"qsize": part.qsize, "offset": part.offset}
        return result

    def total_produced(self) -> int:
        return sum(p.offset for p in self.partitions)

    def total_queued(self) -> int:
        return sum(p.qsize for p in self.partitions)
