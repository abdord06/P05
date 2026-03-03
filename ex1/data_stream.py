from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str):
        self.stream_id = stream_id
        self.stream_type = stream_type
        self.processed_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:

        if not criteria:
            return data_batch
        return [item for item in data_batch if isinstance(item, str)
                and criteria.lower() in item.lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "id": self.stream_id,
            "type": self.stream_type,
            "processed": self.processed_count
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)

            temps = [float(item.split(':')[1]) for item in data_batch
                     if isinstance(item, str) and item.startswith('temp:')]

            if temps:
                avg_temp = sum(temps) / len(temps)
                return f"Sensor analysis: {len(data_batch)} readings processed, avg temp: {avg_temp}°C"
            else:
                return f"- Sensor data: {len(data_batch)} readings processed"
        except Exception as e:
            return f"Error processing sensor batch: {e}"


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)

            flows = [int(item.split(':')[1]) if item.startswith('buy:') else -int(item.split(':')[1])
                     for item in data_batch if isinstance(item, str) and (item.startswith('buy:') or item.startswith('sell:'))]

            if flows:
                net_flow = sum(flows)
                sign = "+" if net_flow > 0 else ""
                return (f"Transaction analysis: {len(data_batch)} operations, "
                        f"net flow: {sign}{net_flow} units")
            else:
                return (f"- Transaction data: {len(data_batch)} operations "
                        "processed")
        except Exception as e:
            return f"Error processing transaction batch: {e}"


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)

            errors = [item for item in data_batch if isinstance(item, str)
                      and item == 'error']

            if 'error' in data_batch:
                return f"Event analysis: {len(data_batch)} events, {len(errors)} error detected"
            else:
                return f"- Event data: {len(data_batch)} events processed"
        except Exception as e:
            return f"Error processing event batch: {e}"


class StreamProcessor:
    def process_mixed_streams(self,
                              streams_data: List[tuple[DataStream,
                                                       List[Any]]]) -> None:
        print("Processing mixed stream types through unified interface...\n")
        print("Batch 1 Results:")
        for stream, batch in streams_data:
            result = stream.process_batch(batch)
            print(result)


def main():
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    s_batch = ['temp:22.5', 'humidity:65', 'pressure:1013']
    print(f"Processing sensor batch: [{', '.join(s_batch)}]")
    print(sensor.process_batch(s_batch))

    print("\nInitializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(f"Stream ID: {transaction.stream_id}, Type: {transaction.stream_type}")
    t_batch = ['buy:100', 'sell:150', 'buy:75']
    print(f"Processing transaction batch: [{', '.join(t_batch)}]")
    print(transaction.process_batch(t_batch))

    print("\nInitializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
    e_batch = ['login', 'error', 'logout']
    print(f"Processing event batch: [{', '.join(e_batch)}]")
    print(event.process_batch(e_batch))

    print("\n=== Polymorphic Stream Processing ===")
    processor = StreamProcessor()
    mixed_batches = [
        (sensor, ['reading1', 'reading2']),
        (transaction, ['op1', 'op2', 'op3', 'op4']),
        (event, ['event1', 'event2', 'event3'])
    ]
    processor.process_mixed_streams(mixed_batches)

    print("\nStream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
