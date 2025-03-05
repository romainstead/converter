import csv
from pathlib import Path
from typing import List

from flight_model import Flight


class CSVWriter:
    def __init__(self, output_file: str):
        self.output_file = Path(output_file)
        self.output_dir = self.output_file.parent
        self.output_dir.mkdir(exist_ok=True)
        self.fieldnames = [field.name for field in Flight.__dataclass_fields__.values()]
        self.is_first_write = True

    async def append_flights(self, flights: List[Flight], include_zero_price: bool = True):
        mode = 'w' if self.is_first_write else 'a'
        with open(self.output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            if self.is_first_write:
                writer.writeheader()
                self.is_first_write = False
            filtered_flights = [flight.__dict__ for flight in flights if include_zero_price or flight.price_ex > 0]
            writer.writerows(filtered_flights)
