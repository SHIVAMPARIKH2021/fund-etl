from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Optional
import pika
import uuid6

from fund_etl.config import settings


@dataclass
class FundEtlPayload:
    year: str
    quarter: str
    isForcedImport: bool
    error: str = ""
    recordsProcessed: int = 0
    status: str = "SUCCESS"


@dataclass
class EtlCompletedEvent:
    payload: FundEtlPayload
    jobId: Optional[str] = None
    timestamp: Optional[str] = None
    correlationId: Optional[str] = None
    specVersion: str = "1.0"
    eventType: str = "com.sec.pipeline.fund.etl.completed"
    source: str = "fund-etl:sec-edgar-worker"

    def __post_init__(self):
        if not self.jobId:
            self.jobId = str(uuid6.uuid7())
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()
        if not self.correlationId:
            self.correlationId = f"sec-ingest-{self.payload.year}-Q{self.payload.quarter}-{uuid6.uuid7().hex[:8]}"


def publish_completion_event(
    payload: FundEtlPayload,
    rmq_host: str,
    rmq_port: int = 5672
):
    rmq_user = settings.rabbitmq_user
    rmq_password = settings.rabbitmq_password
    credentials = pika.PlainCredentials(rmq_user, rmq_password)
    event = EtlCompletedEvent(payload=payload)
    message_body = json.dumps(asdict(event))

    parameters = pika.ConnectionParameters(
        host=rmq_host,
        port=rmq_port,
        virtual_host="/",
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)

    try:
        channel = connection.channel()
        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,  # Persistent delivery
            message_id=event.jobId,
            correlation_id=event.correlationId,
            timestamp=int(datetime.now(timezone.utc).timestamp()),
        )

        channel.basic_publish(
            exchange="etl.events.exchange",
            routing_key="fund.etl.completed",
            body=message_body.encode("utf-8"),
            properties=properties,
        )
    finally:
        connection.close()