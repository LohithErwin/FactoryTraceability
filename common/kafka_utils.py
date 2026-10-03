import json, logging, signal
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer
import config

log = logging.getLogger("kafka")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


def utc_now():
    return datetime.now(timezone.utc)


def make_producer():
    return Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})


def send(producer, topic, event, key=None):
    producer.produce(topic, key=key, value=json.dumps(event).encode())
    producer.poll(0)


def run_consumer(topic, group, handler, on_tick=None):
    """Poll loop: handler(event_dict) per message, on_tick() after every poll."""
    c = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([topic])
    state = {"run": True}

    def stop(*_):
        state["run"] = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log.info("consumer '%s' listening on %s", group, topic)
    try:
        while state["run"]:
            msg = c.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.warning("kafka: %s", msg.error())
                else:
                    try:
                        handler(json.loads(msg.value()))
                    except Exception:
                        log.exception("failed handling %s", msg.value())
            if on_tick:
                try:
                    on_tick()
                except Exception:
                    log.exception("tick failed")
    finally:
        c.close()
