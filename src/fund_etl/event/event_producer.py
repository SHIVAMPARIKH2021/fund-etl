import json
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
import pika

@dataclass
class FundEtlPayload:
    year: int
    quarter: int
    ticker: str = "True"
    force: bool = False
    recordsProcessed: int = 0
    status: str = "SUCCESS"

@dataclass
class EtlCompletedEvent:
    payload: FundEtlPayload
    eventId: str = ""
    eventType: str = "com.sec.pipeline.fund.etl.completed"
    specVersion: str = "1.0"
    source: str = "fund-etl:sec-edgar-worker"
    correlationId: str = ""
    timestamp: str = ""

    def __post_init__(self):
        if not self.eventId:
            self.eventId = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()
        if not self.correlationId:
            self.correlationId = f"corr-{self.payload.year}-Q{self.payload.quarter}-{uuid.uuid4().hex[:8]}"

def publish_completion_event(payload: FundEtlPayload, rmq_host: str = "localhost"):
    event = EtlCompletedEvent(payload=payload)
    message_body = json.dumps(asdict(event))

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rmq_host))
    channel = connection.channel()

    # Topic Exchange
    channel.basic_publish(
        exchange="etl.events.exchange",
        routing_key="fund.etl.completed",
        body=message_body.encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,  # Persistent storage on disk
            message_id=event.eventId,
            correlation_id=event.correlationId,
            timestamp=int(datetime.now(timezone.utc).timestamp())
        )
    )
    connection.close()