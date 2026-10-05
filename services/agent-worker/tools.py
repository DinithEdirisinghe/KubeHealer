# services/agent-worker/tools.py
# Real read-only diagnostic tools exposed to the LangGraph LLM Agent
import os
import json
import yaml
import requests
from typing import Dict, Any
from kubernetes import client, config

# Initialize Kubernetes Client
try:
    config.load_incluster_config()
except Exception:
    try:
        config.load_kube_config()
    except Exception:
        pass

core_api = client.CoreV1Api()
apps_api = client.AppsV1Api()

def tool_get_pod_logs(namespace: str, pod: str) -> str:
    """Fetches container stdout/stderr logs for a specific pod in a namespace.
    Checks both current logs and previous crashed instance logs.
    """
    try:
        # Try reading previous crashed container logs first
        try:
            logs = core_api.read_namespaced_pod_log(
                name=pod,
                namespace=namespace,
                previous=True,
                tail_lines=100
            )
            if logs:
                return f"--- PREVIOUS CRASHED LOGS ---\n{logs}"
        except Exception:
            pass

        # Fallback to current pod logs
        logs = core_api.read_namespaced_pod_log(
            name=pod,
            namespace=namespace,
            tail_lines=100
        )
        return f"--- CURRENT LOGS ---\n{logs}"
    except Exception as e:
        return f"Error fetching logs for pod {pod} in {namespace}: {str(e)}"

def tool_get_pod_events(namespace: str, pod: str) -> str:
    """Fetches Kubernetes Warning and Normal event logs for a specific pod."""
    try:
        events = core_api.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.name={pod}"
        )
        output = []
        for event in events.items:
            output.append(f"[{event.type}] {event.reason}: {event.message} (Count: {event.count})")
        if not output:
            return f"No events found for pod {pod} in namespace {namespace}."
        return "\n".join(output)
    except Exception as e:
        return f"Error fetching events for pod {pod} in {namespace}: {str(e)}"

def tool_get_deployment_manifest(namespace: str, deployment: str) -> str:
    """Fetches current YAML manifest specification of a target Deployment."""
    try:
        dep = apps_api.read_namespaced_deployment(name=deployment, namespace=namespace)
        dep_dict = client.ApiClient().sanitize_for_serialization(dep)
        # Filter down spec for concise LLM context
        clean_spec = {
            "metadata": {
                "name": dep_dict["metadata"]["name"],
                "namespace": dep_dict["metadata"]["namespace"]
            },
            "spec": {
                "replicas": dep_dict["spec"].get("replicas", 1),
                "template": {
                    "spec": {
                        "containers": dep_dict["spec"]["template"]["spec"].get("containers", [])
                    }
                }
            }
        }
        return yaml.dump(clean_spec, default_flow_style=False)
    except Exception as e:
        return f"Error fetching deployment manifest for {deployment} in {namespace}: {str(e)}"

def tool_query_prometheus(promql: str) -> str:
    """Queries live Prometheus metric server using PromQL query syntax."""
    prom_url = os.getenv("PROMETHEUS_URL", "http://prometheus-prometheus-kube-prometheus-prometheus.kubehealer-infra.svc.cluster.local:9090")
    try:
        response = requests.get(f"{prom_url}/api/v1/query", params={"query": promql}, timeout=5)
        if response.status_code == 200:
            return json.dumps(response.json().get("data", {}), indent=2)
        return f"Prometheus query failed with status code: {response.status_code}"
    except Exception as e:
        return f"Error querying Prometheus ({promql}): {str(e)}"
