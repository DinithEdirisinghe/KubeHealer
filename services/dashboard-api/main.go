package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	executorv1 "github.com/kubehealer/kubehealer/services/dashboard-api/proto/executor/v1"
)

type IncidentProposal struct {
	IncidentID        string `json:"incident_id"`
	Namespace         string `json:"namespace"`
	TargetName        string `json:"target_name"`
	DiagnosisSummary  string `json:"diagnosis_summary"`
	RiskScore         string `json:"risk_score"`
	ProposedPatchYAML string `json:"proposed_patch_yaml"`
	CreatedAtUnix     int64  `json:"created_at_unix"`
	Status            string `json:"status"` // PENDING, EXECUTED, REJECTED
}

type DiagnosticLog struct {
	IncidentID    string `json:"incident_id"`
	StepType      string `json:"step_type"`
	AgentName     string `json:"agent_name"`
	Message       string `json:"message"`
	TimestampUnix int64  `json:"timestamp_unix"`
}

type IncidentHub struct {
	mu        sync.RWMutex
	proposals map[string]*IncidentProposal
	logs      []DiagnosticLog
	clients   map[chan string]bool
}

var hub = &IncidentHub{
	proposals: make(map[string]*IncidentProposal),
	logs:      make([]DiagnosticLog, 0),
	clients:   make(map[chan string]bool),
}

func (h *IncidentHub) broadcast(data string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

func main() {
	port := getEnv("PORT", "8081")
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka.kubehealer-infra.svc.cluster.local:9092")
	executorAddr := getEnv("EXECUTOR_GRPC_ADDR", "k8s-executor.kubehealer-system.svc.cluster.local:50051")

	// Start Kafka Consumers in Background
	go consumeDiagnostics(kafkaBroker)
	go consumeProposals(kafkaBroker)

	router := gin.Default()

	// Enable CORS for web frontend
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})

	router.GET("/api/v1/incidents", func(c *gin.Context) {
		hub.mu.RLock()
		defer hub.mu.RUnlock()

		list := make([]*IncidentProposal, 0, len(hub.proposals))
		for _, p := range hub.proposals {
			list = append(list, p)
		}
		c.JSON(http.StatusOK, gin.H{
			"incidents": list,
			"logs":      hub.logs,
		})
	})

	// Server-Sent Events (SSE) Endpoint
	router.GET("/api/v1/stream", func(c *gin.Context) {
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Header().Set("Transfer-Encoding", "chunked")

		messageChan := make(chan string, 50)

		hub.mu.Lock()
		hub.clients[messageChan] = true
		hub.mu.Unlock()

		defer func() {
			hub.mu.Lock()
			delete(hub.clients, messageChan)
			close(messageChan)
			hub.mu.Unlock()
		}()

		// Immediate handshake event so browser onopen triggers instantly
		c.SSEvent("ping", "connected")
		c.Writer.Flush()

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		c.Stream(func(w io.Writer) bool {
			select {
			case msg, ok := <-messageChan:
				if !ok {
					return false
				}
				c.SSEvent("message", msg)
				return true
			case <-ticker.C:
				c.SSEvent("ping", "heartbeat")
				return true
			case <-c.Request.Context().Done():
				return false
			}
		})
	})

	// Human Approval Gate Endpoint
	router.POST("/api/v1/incidents/:id/approve", func(c *gin.Context) {
		incidentID := c.Param("id")

		hub.mu.Lock()
		proposal, exists := hub.proposals[incidentID]
		hub.mu.Unlock()

		if !exists {
			c.JSON(http.StatusNotFound, gin.H{"error": "Incident proposal not found"})
			return
		}

		// Connect to k8s-executor via gRPC using JSON codec
		conn, err := grpc.NewClient(
			executorAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")),
		)
		if err != nil {
			log.Printf("[Approve ERROR] Failed to connect to k8s-executor: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to connect to k8s-executor: %v", err)})
			return
		}
		defer conn.Close()

		client := executorv1.NewExecutionServiceClient(conn)

		// Execute ApplyPatch RPC
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		log.Printf("[Approve] Dispatching ApplyPatch via gRPC to %s for target %s/%s", executorAddr, proposal.Namespace, proposal.TargetName)

		resp, err := client.ApplyPatch(ctx, &executorv1.ApplyPatchRequest{
			IncidentId:   proposal.IncidentID,
			Namespace:    proposal.Namespace,
			TargetKind:   "Deployment",
			TargetName:   proposal.TargetName,
			PatchType:    "application/strategic-merge-patch+json",
			PatchPayload: []byte(proposal.ProposedPatchYAML),
		})
		if err != nil {
			log.Printf("[Approve ERROR] gRPC ApplyPatch failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("gRPC ApplyPatch failed: %v", err)})
			return
		}

		hub.mu.Lock()
		proposal.Status = "EXECUTED"
		hub.mu.Unlock()

		// Broadcast execution result to UI stream
		execLog := fmt.Sprintf(`{"type":"execution","incident_id":"%s","message":"%s","status":"EXECUTED"}`, incidentID, resp.Message)
		hub.broadcast(execLog)

		c.JSON(http.StatusOK, gin.H{
			"success": resp.Success,
			"message": resp.Message,
		})
	})

	log.Printf("Starting dashboard-api on port %s...", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func consumeDiagnostics(broker string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "incident.diagnostics.stream",
		GroupID:     "dashboard-api-diag-group",
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		var logEntry DiagnosticLog
		if err := json.Unmarshal(m.Value, &logEntry); err == nil {
			log.Printf("[Diagnostics] %s: %s", logEntry.StepType, logEntry.Message)
			hub.mu.Lock()
			hub.logs = append(hub.logs, logEntry)
			if len(hub.logs) > 200 {
				hub.logs = hub.logs[len(hub.logs)-200:]
			}
			hub.mu.Unlock()

			eventData, _ := json.Marshal(map[string]interface{}{
				"type": "diagnostic",
				"data": logEntry,
			})
			hub.broadcast(string(eventData))
		}
	}
}

func consumeProposals(broker string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "incident.remediation.proposals",
		GroupID:     "dashboard-api-prop-group",
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		var prop IncidentProposal
		if err := json.Unmarshal(m.Value, &prop); err == nil {
			log.Printf("[Proposal] Ingested proposal for %s (%s)", prop.IncidentID, prop.TargetName)
			prop.Status = "PENDING"
			hub.mu.Lock()
			hub.proposals[prop.IncidentID] = &prop
			hub.mu.Unlock()

			eventData, _ := json.Marshal(map[string]interface{}{
				"type": "proposal",
				"data": prop,
			})
			hub.broadcast(string(eventData))
		}
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
