from dataclasses import dataclass
from datetime import datetime


@dataclass
class Flight:
    export_date: datetime
    origin: str
    destination: str
    is_one_way: bool
    carrier: str
    aircraft_code: str
    aircraft_name: str
    flight_number: str
    outbound_departure_date: str
    outbound_arrival_date: str
    price_ex: float
    tax: float
    currency: str
    transfer_iata: str | None
    transfer_duration: int