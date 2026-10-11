"""Poll Finnhub for five stock quotes and publish them to Kafka."""
import json
import os
import time

import requests
from kafka import KafkaProducer

API_KEY = os.environ["FINNHUB_API_KEY"]
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def fetch_quote(symbol):
    try:
        response = requests.get(
            BASE_URL, params={"symbol": symbol, "token": API_KEY}, timeout=10
        )
        response.raise_for_status()
        quote = response.json()
        if quote.get("c") in (None, 0):
            print(f"No usable current price for {symbol}: {quote}")
            return None
        quote["symbol"] = symbol
        quote["fetched_at"] = int(time.time())
        return quote
    except (requests.RequestException, ValueError) as exc:
        print(f"Error fetching {symbol}: {exc}")
        return None


def main():
    print(f"Publishing {SYMBOLS} to Kafka topic {TOPIC}")
    try:
        while True:
            for symbol in SYMBOLS:
                quote = fetch_quote(symbol)
                if quote:
                    producer.send(TOPIC, value=quote)
                    print(f"Published {symbol}: {quote}")
            producer.flush()
            time.sleep(6)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
