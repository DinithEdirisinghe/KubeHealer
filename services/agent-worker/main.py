# services/agent-worker/main.py
# Python Kafka Consumer & LangGraph Orchestrator Worker
import os
import time
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from graph import build_incident_graph, IncidentState

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka.kubehealer-infra.svc.cluster.local:9092")
RAW_ALERTS_TOPIC = "incident.alerts.raw"
DIAGNOSTICS_STREAM_TOPIC = "incident.diagnostics.stream"
PROPOSALS_TOPIC = "incident.remediation.proposals"

def create_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                RAW_ALERTS_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="agent-worker-group",
                auto_offset_reset="latest"
            )
            logging.info(f"Successfully connected Kafka Consumer to broker: {KAFKA_BROKER}")
            return consumer
        except Exception as e:
            logging.warning(f"Waiting for Kafka broker ({KAFKA_BROKER})... Error: {e}")
            time.sleep(5)

def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logging.info(f"Successfully connected Kafka Producer to broker: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            logging.warning(f"Waiting for Kafka Producer ({KAFKA_BROKER})... Error: {e}")
            time.sleep(5)

def main():
    logging.info("Starting KubeHealer agent-worker Python Microservice...")
    
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    graph_app = build_incident_graph()

    logging.info(f"Listening for incidents on Kafka topic: {RAW_ALERTS_TOPIC}...")

    for message in consumer:
        try:
            alert_data = message.value
            logging.info(f"Received raw alert message from Kafka: {alert_data.get('incident_id')}")

            # Initial State Construction
            initial_state: IncidentState = {
                "incident_id": alert_data.get("incident_id", "inc-unknown"),
                "fingerprint": alert_data.get("fingerprint", ""),
                "alert_name": alert_data.get("alert_name", "PodFailure"),
                "severity": alert_data.get("severity", "critical"),
                "namespace": alert_data.get("namespace", "sandbox-apps"),
                "pod_name": alert_data.get("pod_name", ""),
                "deployment_name": alert_data.get("deployment_name", alert_data.get("pod_name", "")),
                "logs": "",
                "events": "",
                "manifest": "",
                "diagnosis": "",
                "proposed_patch_yaml": "",
                "risk_score": "Low",
                "human_approved": False,
                "status": "INGESTED"
            }

            # Stream step 1: Diagnostic start
            producer.send(DIAGNOSTICS_STREAM_TOPIC, {
                "incident_id": initial_state["incident_id"],
                "step_type": "thought",
                "agent_name": "TriageAgent",
                "message": f"Ingested incident alert '{initial_state['alert_name']}' for target '{initial_state['deployment_name']}'. Starting diagnostic graph execution...",
                "timestamp_unix": int(time.time())
            })

            # Run LangGraph State Machine
            final_state = graph_app.invoke(initial_state)

            # Stream step 2: Diagnostic complete
            producer.send(DIAGNOSTICS_STREAM_TOPIC, {
                "incident_id": final_state["incident_id"],
                "step_type": "remediation_proposal",
                "agent_name": "RemediationAgent",
                "message": f"Diagnosis complete: {final_state['diagnosis']}",
                "timestamp_unix": int(time.time())
            })

            # Publish proposal to proposals topic
            producer.send(PROPOSALS_TOPIC, {
                "incident_id": final_state["incident_id"],
                "namespace": final_state["namespace"],
                "target_name": final_state["deployment_name"],
                "diagnosis_summary": final_state["diagnosis"],
                "risk_score": final_state["risk_score"],
                "proposed_patch_yaml": final_state["proposed_patch_yaml"],
                "created_at_unix": int(time.time())
            })
            producer.flush()

            logging.info(f"Successfully processed incident {final_state['incident_id']}. Proposal published!")

        except Exception as e:
            logging.error(f"Error processing Kafka alert message: {e}", exc_info=True)

if __name__ == "__main__":
    main()
