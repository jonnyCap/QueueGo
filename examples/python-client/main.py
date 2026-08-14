#!/usr/bin/env python3
"""
Python QueueGo / Blink Example: Real-Time Event Publisher & Subscriber
"""

import os
import sys
import time
import json
import hmac
import hashlib
import base64
import asyncio

# Add Blink python package path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../Blink/python")))
try:
    from blink import BlinkClient, hash_topic  # type: ignore  # noqa: E402
except ImportError:
    from Blink.python.blink import BlinkClient, hash_topic  # type: ignore  # noqa: E402


def generate_token(signing_key: str, sub: str, queue_key: str, *permissions: str) -> str:
    """Generates a signed HS256 JWT using Python standard library (no dependencies needed)."""
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": sub,
        "queue_key": queue_key,
        "permissions": list(permissions),
    }

    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

    header_b64 = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")

    sig = hmac.new(signing_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    sig_b64 = b64url(sig)

    return f"{header_b64}.{payload_b64}.{sig_b64}"


async def main():
    broker_host = "127.0.0.1"
    broker_port = 9000
    master_key = "my-master-key"
    queue_key = "python-demo-secret"
    topic_name = "python-events"

    print("==================================================")
    print(" QueueGo Python Client (Blink Protocol) Example")
    print("==================================================")

    # 1. Generate auth tokens
    create_token = generate_token(master_key, "python-admin", queue_key, "create")
    client_token = generate_token(master_key, "python-user", queue_key, "subscribe", "publish")

    # 2. Connect to broker
    print(f"\n[1/4] Connecting to QueueGo broker at {broker_host}:{broker_port}...")
    client = BlinkClient(host=broker_host, port=broker_port)
    try:
        await client.connect()
    except Exception as e:
        print(f"Error: Failed to connect to broker at {broker_host}:{broker_port}. Is QueueGo running?")
        print(f"Start it with: go run cmd/queuego/main.go")
        return

    print("✓ Connected successfully!")

    # 3. Create topic
    print(f"\n[2/4] Registering topic {topic_name!r} on broker...")
    topic_id = await client.create_topic(create_token, topic_name)
    print(f"✓ Topic registered! (Hashed ID: {topic_id})")

    # 4. Subscribe with callback handler
    print(f"\n[3/4] Subscribing to topic {topic_name!r}...")
    messages_received = []

    def on_message(t_id: int, payload: bytes):
        try:
            data = json.loads(payload.decode("utf-8"))
            print(f"\n  [Subscriber Received] TopicID: {t_id} | Event: {data.get('event')} | Msg: {data.get('message')}")
        except Exception:
            print(f"\n  [Subscriber Received] Raw payload: {payload.decode('utf-8', errors='replace')}")
        messages_received.append(payload)

    await client.subscribe(client_token, topic_name, on_message)
    print("✓ Subscribed and listening for incoming messages!")

    # 5. Publish test events
    print(f"\n[4/4] Publishing events to {topic_name!r}...")
    for i in range(1, 4):
        event_payload = json.dumps({
            "event": f"PAYMENT_COMPLETED_{i}",
            "amount": 49.99 * i,
            "currency": "EUR",
            "message": f"Payment #{i} processed successfully via Python SDK",
            "timestamp": time.time(),
        })
        await client.publish(client_token, topic_name, event_payload)
        print(f"  -> Published event #{i}")
        await asyncio.sleep(0.1)

    # Wait briefly to ensure all dispatches are handled
    await asyncio.sleep(0.3)

    print(f"\n✓ Completed! Total messages received by subscriber: {len(messages_received)}")
    await client.close()
    print("✓ Connection closed gracefully.\n")


if __name__ == "__main__":
    asyncio.run(main())
