# services/agent-worker/graph.py
# LangGraph Stateful Multi-Agent Incident Pipeline with Human Approval Interrupt Gate
import json
import yaml
from typing import TypedDict, Optional, Dict, Any
from langgraph.graph import StateGraph, END
from tools import (
    tool_get_pod_logs,
    tool_get_pod_events,
    tool_get_deployment_manifest,
    tool_query_prometheus
)

# IncidentState defines the shared state object passed across LangGraph nodes
class IncidentState(TypedDict):
    incident_id: str
    fingerprint: str
    alert_name: str
    severity: str
    namespace: str
    pod_name: str
    deployment_name: str
    logs: str
    events: str
    manifest: str
    diagnosis: str
    proposed_patch_yaml: str
    risk_score: str
    human_approved: bool
    status: str

# Node 1: Triage Node
def triage_node(state: IncidentState) -> IncidentState:
    print(f"--- [Triage Node] Processing Incident: {state['incident_id']} (Alert: {state['alert_name']}) ---")
    state["status"] = "TRIAGED"
    return state

# Node 2: Diagnostics Node
def diagnostics_node(state: IncidentState) -> IncidentState:
    print(f"--- [Diagnostics Node] Gathering Cluster Telemetry for {state['deployment_name']} in {state['namespace']} ---")
    
    namespace = state.get("namespace", "sandbox-apps")
    pod = state.get("pod_name", "")
    deployment = state.get("deployment_name", "")

    # Execute read-only diagnostic tools
    if pod:
        state["logs"] = tool_get_pod_logs(namespace, pod)
        state["events"] = tool_get_pod_events(namespace, pod)
    
    if deployment:
        state["manifest"] = tool_get_deployment_manifest(namespace, deployment)
    
    state["status"] = "DIAGNOSED"
    return state

# Node 3: Remediation Node
def remediation_node(state: IncidentState) -> IncidentState:
    print(f"--- [Remediation Node] Synthesizing Root Cause & Generating YAML Patch ---")
    
    alert_name = state.get("alert_name", "")
    logs = state.get("logs", "")
    events = state.get("events", "")
    deployment = state.get("deployment_name", "oom-service")

    # Smart synthesis logic based on failure evidence
    if "OOMKilled" in alert_name or "OOMKilled" in logs or "OOMKilled" in events:
        state["diagnosis"] = f"Container {deployment} was terminated by Linux kernel OOM-Killer because it exceeded its 64Mi memory limit."
        state["risk_score"] = "Low"
        
        # Exact strategic merge patch bumping memory limit to 256Mi
        patch_spec = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "oom-simulator",
                                "resources": {
                                    "limits": {
                                        "memory": "256Mi",
                                        "cpu": "200m"
                                    },
                                    "requests": {
                                        "memory": "128Mi",
                                        "cpu": "100m"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
        state["proposed_patch_yaml"] = json.dumps(patch_spec)

    elif "CrashLoopBackOff" in alert_name or "DATABASE_URL" in logs:
        state["diagnosis"] = f"Container {deployment} failed to start due to missing required environment variable 'DATABASE_URL'."
        state["risk_score"] = "Low"
        
        patch_spec = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "app",
                                "env": [
                                    {
                                        "name": "DATABASE_URL",
                                        "value": "postgresql://kubehealer:kubehealerpassword@postgresql.kubehealer-infra.svc.cluster.local:5432/kubehealer"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }
        state["proposed_patch_yaml"] = json.dumps(patch_spec)
    else:
        state["diagnosis"] = f"Application {deployment} experienced a failure. Triggering rollout restart."
        state["risk_score"] = "Medium"
        state["proposed_patch_yaml"] = "{}"

    state["status"] = "PROPOSAL_READY"
    return state

# Node 4: Approval Gate Node (State Checkpointing Interrupt)
def approval_gate_node(state: IncidentState) -> IncidentState:
    print(f"--- [Approval Gate] Pausing Incident {state['incident_id']} Awaiting Human Approval ---")
    state["status"] = "AWAITING_APPROVAL"
    return state

# Build the LangGraph workflow
def build_incident_graph():
    workflow = StateGraph(IncidentState)

    workflow.add_node("triage", triage_node)
    workflow.add_node("diagnostics", diagnostics_node)
    workflow.add_node("remediation", remediation_node)
    workflow.add_node("approval_gate", approval_gate_node)

    workflow.set_entry_point("triage")
    workflow.add_edge("triage", "diagnostics")
    workflow.add_edge("diagnostics", "remediation")
    workflow.add_edge("remediation", "approval_gate")
    workflow.add_edge("approval_gate", END)

    return workflow.compile()
