"""Write Kafka quote messages as JSON objects in SeaweedFS S3."""
import json
import os
import time
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer

BUCKET = os.getenv("S3_BUCKET", "bronze-transactions")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9002"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            s3.create_bucket(Bucket=BUCKET)
        else:
            raise  # Do not treat permission problems as a missing bucket.

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze-consumer1",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    print(f"Consuming {TOPIC} and saving into s3://{BUCKET}/")
    try:
        for message in consumer:
            record = message.value
            symbol = str(record.get("symbol") or "unknown")
            timestamp = int(record.get("fetched_at") or time.time())
            # UUID suffix prevents accidentally overwriting quotes from the same second.
            key = f"{symbol}/{timestamp}_{uuid4().hex[:12]}.json"
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=json.dumps(record).encode("utf-8"),
                ContentType="application/json",
            )
            print(f"Saved {symbol} -> s3://{BUCKET}/{key}")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
