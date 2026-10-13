
import hashlib
import hmac
import json
import os
import time
import urllib.request
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

HERMES_URL = os.environ["HERMES_URL"]
SECRET = os.environ["HERMES_SECRET"].encode()

config.load_incluster_config()
api = client.CoreV1Api()
apps_api = client.AppsV1Api()


def _owner_reference(resource, kind):
    for owner in resource.get("metadata", {}).get("ownerReferences", []):
        if owner.get("kind") == kind:
            return owner
    return None


def _pod_context(pod):
    pod_data = client.ApiClient().sanitize_for_serialization(pod)
    metadata = pod_data.get("metadata", {})
    spec = pod_data.get("spec", {})
    status = pod_data.get("status", {})
    conditions = status.get("conditions", [])
    container_statuses = {
        container.get("name"): container
        for container in status.get("containerStatuses", [])
    }

    containers = []
    for container in spec.get("containers", []):
        container_status = container_statuses.get(container.get("name"), {})
        state = container_status.get("state", {})
        current_state = next(iter(state), None)
        state_details = state.get(current_state, {}) if current_state else {}
        containers.append(
            {
                "name": container.get("name"),
                "image": container.get("image"),
                "ready": container_status.get("ready"),
                "restart_count": container_status.get("restartCount"),
                "state": current_state,
                "state_reason": state_details.get("reason"),
                "exit_code": state_details.get("exitCode"),
            }
        )

    return {
        "name": metadata.get("name"),
        "namespace": metadata.get("namespace"),
        "uid": metadata.get("uid"),
        "labels": metadata.get("labels", {}),
        "node_name": spec.get("nodeName"),
        "phase": status.get("phase"),
        "ready": any(
            condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        ),
        "containers": containers,
    }


def _deployment_context(deployment):
    deployment_data = client.ApiClient().sanitize_for_serialization(deployment)
    metadata = deployment_data.get("metadata", {})
    spec = deployment_data.get("spec", {})
    status = deployment_data.get("status", {})
    pod_template = spec.get("template", {})
    pod_spec = pod_template.get("spec", {})

    return {
        "name": metadata.get("name"),
        "namespace": metadata.get("namespace"),
        "uid": metadata.get("uid"),
        "generation": metadata.get("generation"),
        "observed_generation": status.get("observedGeneration"),
        "labels": metadata.get("labels", {}),
        "selector": spec.get("selector", {}).get("matchLabels", {}),
        "replicas": {
            "desired": spec.get("replicas", 1),
            "total": status.get("replicas", 0),
            "ready": status.get("readyReplicas", 0),
            "available": status.get("availableReplicas", 0),
            "updated": status.get("updatedReplicas", 0),
        },
        "images": [
            container.get("image")
            for container in pod_spec.get("containers", [])
        ],
    }


def _workload_context(involved):
    if involved.get("kind") != "Pod":
        return None

    namespace = involved.get("namespace") or "default"
    pod_name = involved.get("name")
    context = {
        "pod": {
            "name": pod_name,
            "namespace": namespace,
            "uid": involved.get("uid"),
        },
        "deployment": None,
    }

    try:
        pod = api.read_namespaced_pod(name=pod_name, namespace=namespace)
        context["pod"] = _pod_context(pod)
        pod_data = client.ApiClient().sanitize_for_serialization(pod)
        owner = _owner_reference(pod_data, "ReplicaSet")

        if owner:
            replica_set = apps_api.read_namespaced_replica_set(
                name=owner["name"], namespace=namespace
            )
            replica_set_data = client.ApiClient().sanitize_for_serialization(
                replica_set
            )
            owner = _owner_reference(replica_set_data, "Deployment")

        if owner:
            deployment = apps_api.read_namespaced_deployment(
                name=owner["name"], namespace=namespace
            )
            context["deployment"] = _deployment_context(deployment)
    except ApiException as exc:
        context["lookup_error"] = (
            f"Kubernetes API returned HTTP {exc.status} while resolving workload"
        )
        print(context["lookup_error"], flush=True)

    return context



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
        "action": kube_event.get("action"),
        "count": kube_event.get("count"),
        "first_timestamp": kube_event.get("firstTimestamp"),
        "last_timestamp": kube_event.get("lastTimestamp"),
        "event_time": kube_event.get("eventTime"),
        "reporting_component": kube_event.get("reportingComponent"),
        "reporting_instance": kube_event.get("reportingInstance"),
        "involved_object": {
            "kind": involved.get("kind"),
            "name": involved.get("name"),
            "namespace": involved.get("namespace"),
            "uid": involved.get("uid"),
            "api_version": involved.get("apiVersion"),
            "field_path": involved.get("fieldPath"),
        },
        "workload": _workload_context(involved),
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
