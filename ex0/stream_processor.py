from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        return all(isinstance(x, (int, float)) for x in data)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Error: data is not a number")
            total = sum(data)
            avg = total / len(data) if len(data) > 0 else 0
            return (f"Output: Processed {len(data)} numeric values, "
                    f"sum={total}, avg={avg:.1f}")

        except Exception as e:
            return f"Error in NumericProcessor: {e}"

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate:
                raise ValueError("Erorr: data is not text")
            chars = len(data)
            words = len(data.split())
            return f"Output: Processed text: {chars} characters, {words} words"
        except Exception as e:
            return f"Error in TextProcessor: {e}"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ":" in data

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid llog format. must contain ':'.")
            level, message = data.split(":", 1)
            level = level.strip()
            message = message.strip()
            if level == "ERROR":
                return f"[ALERT] {level} level detected: {message}"
            else:
                return f"[{level}] {level} level detected: {message}"
        except Exception as e:
            return f"Error in log processor : {e}"


def main():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    num_proc = NumericProcessor()
    data_num = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_num}")
    if num_proc.validate(data_num):
        print("Validation: Numeric data verified")
        print(num_proc.format_output(num_proc.process(data_num)))

    print("\nInitializing Text Processor...")
    text_proc = TextProcessor()
    data_text = "Hello Nexus World"
    print(f"Processing data: \"{data_text}\"")
    if text_proc.validate(data_text):
        print("Validation: Text data verified")
        print(text_proc.format_output(text_proc.process(data_text)))

    print("\nInitializing Log Processor...")
    log_proc = LogProcessor()
    data_log = "ERROR: Connection timeout"
    print(f"Processing data: \"{data_log}\"")
    if log_proc.validate(data_log):
        print("Validation: Log entry verified")
        print(log_proc.format_output(log_proc.process(data_log)))

    print("\n=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")

    demo_data = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello Nexus!"),
        (LogProcessor(), "INFO: System ready")
    ]

    for i, (processor, data) in enumerate(demo_data, 1):
        result = processor.process(data)
        print(f"Result {i}: {result}")

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
