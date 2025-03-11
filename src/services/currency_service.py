def convert_currency(value: float, current_currency: str, target_currency: str) -> float:
    return value * EXCHANGE_RATES[current_currency] / EXCHANGE_RATES[target_currency]



EXCHANGE_RATES = {
    "EUR": 81.14,
    "USD": 76.4,
    "GBP": 91.9,
    "RUR": 1
}

if __name__ == "__main__":
    print(convert_currency(76.4, "EUR", "RUR"))