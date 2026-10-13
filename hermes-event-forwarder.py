
import hashlib
import hmac
import json
import os
import time
import urllib.request
from kubernetes import client, config, watch

HERMES_URL = os.environ["HERMES_URL"]
SECRET = os.environ["HERMES_SECRET"].encode()

config.load_incluster_config()
api = client.CoreV1Api()



def forward(kube_event):
    metadata = kube_event.get("metadata", {})
    involved = kube_event.get("involvedObject", {})

    payload = {
        "event_type": "kube_event",
        "type": kube_event.get("type", "Normal"),
        "reason": kube_event.get("reason", ""),
        "message": kube_event.get("message", ""),
        "namespace": metadata.get("namespace", "default"),
        "source": "kubernetes",
    }

    body = json.dumps(
        payload,
        separators=(",", ":"),
        ensure_ascii=False
    ).encode("utf-8")

    timestamp = str(int(time.time()))
    signed_data = timestamp.encode() + b"." + body

    signature = hmac.new(
        SECRET, signed_data, hashlib.sha256
    ).hexdigest()

    request = urllib.request.Request(
        HERMES_URL,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature-V2": signature,
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            print(
                f"Hermes accepted event: HTTP {response.status}; "
                f"reason={payload['reason']}",
                flush=True,
            )
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        print(
            f"Hermes HTTP {exc.code}: {response_body}",
            flush=True,
        )
        raise

while True:
    watcher = watch.Watch()
    try:
        for item in watcher.stream(
            api.list_event_for_all_namespaces,
            timeout_seconds=300,
        ):
            obj = item.get("object")
            if not obj:
                continue

            event = client.ApiClient().sanitize_for_serialization(obj)
            event_type = event.get("type", "Normal")
            reason = event.get("reason", "")

            # Forward warnings and selected important events.
            if event_type == "Warning" or reason in {
                "BackOff",
                "FailedScheduling",
                "FailedMount",
                "Unhealthy",
                "NodeNotReady",
            }:
                try:
                    forward(event)
                except Exception as exc:
                    print(f"Forwarding failed: {exc}", flush=True)

    except Exception as exc:
        print(f"Kubernetes watch failed: {exc}", flush=True)
        time.sleep(5)
    finally:
        watcher.stop()
