package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// AlertManagerWebhook represents incoming webhook payload from Prometheus Alertmanager
type AlertManagerWebhook struct {
	Receiver          string            `json:"receiver"`
	Status            string            `json:"status"`
	Alerts            []Alert           `json:"alerts"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
}

type Alert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	EndsAt       string            `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// NormalizedIncident is the standardized schema published to Kafka (incident.alerts.raw)
type NormalizedIncident struct {
	IncidentID     string            `json:"incident_id"`
	Fingerprint    string            `json:"fingerprint"`
	Status         string            `json:"status"`
	AlertName      string            `json:"alert_name"`
	Severity       string            `json:"severity"`
	Namespace      string            `json:"namespace"`
	PodName        string            `json:"pod_name"`
	DeploymentName string            `json:"deployment_name"`
	StartsAt       string            `json:"starts_at"`
	Labels         map[string]string `json:"labels"`
	Annotations    map[string]string `json:"annotations"`
}

var (
	redisClient *redis.Client
	kafkaWriter *kafka.Writer
)

func main() {
	redisHost := getEnv("REDIS_HOST", "redis-master.kubehealer-infra.svc.cluster.local:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "kubehealerredis")
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka.kubehealer-infra.svc.cluster.local:9092")
	port := getEnv("PORT", "8080")

	// Initialize Redis Client
	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: redisPassword,
		DB:       0,
	})

	// Initialize Kafka Writer
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "incident.alerts.raw",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	router := gin.Default()

	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP"})
	})

	router.POST("/api/v1/alerts", handleAlertWebhook)

	log.Printf("Starting alert-gateway on port %s...", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func handleAlertWebhook(c *gin.Context) {
	var payload AlertManagerWebhook
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid webhook payload: " + err.Error()})
		return
	}

	ctx := context.Background()
	processedCount := 0

	for _, alert := range payload.Alerts {
		fingerprint := alert.Fingerprint
		if fingerprint == "" {
			fingerprint = fmt.Sprintf("%s-%s", alert.Labels["alertname"], alert.Labels["pod"])
		}

		// Deduplication check via Redis (60-second TTL window)
		dedupKey := fmt.Sprintf("dedup:alert:%s", fingerprint)
		set, err := redisClient.SetNX(ctx, dedupKey, "locked", 60*time.Second).Result()
		if err != nil {
			log.Printf("Redis warning checking deduplication for fingerprint %s: %v", fingerprint, err)
		} else if !set {
			log.Printf("Duplicate alert skipped for fingerprint: %s", fingerprint)
			continue
		}

		incidentID := fmt.Sprintf("inc-%d-%s", time.Now().Unix(), fingerprint[:min(6, len(fingerprint))])

		namespace := alert.Labels["namespace"]
		if namespace == "" {
			namespace = "sandbox-apps"
		}

		podName := alert.Labels["pod"]
		deploymentName := alert.Labels["deployment"]
		if deploymentName == "" && podName != "" {
			deploymentName = podName
		}

		normalized := NormalizedIncident{
			IncidentID:     incidentID,
			Fingerprint:    fingerprint,
			Status:         alert.Status,
			AlertName:      alert.Labels["alertname"],
			Severity:       alert.Labels["severity"],
			Namespace:      namespace,
			PodName:        podName,
			DeploymentName: deploymentName,
			StartsAt:       alert.StartsAt,
			Labels:         alert.Labels,
			Annotations:    alert.Annotations,
		}

		jsonData, err := json.Marshal(normalized)
		if err != nil {
			log.Printf("Failed to marshal normalized incident: %v", err)
			continue
		}

		// Publish to Kafka topic incident.alerts.raw
		err = kafkaWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(incidentID),
			Value: jsonData,
		})
		if err != nil {
			log.Printf("Failed to publish incident to Kafka: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish to Kafka"})
			return
		}

		log.Printf("Successfully published incident %s (Alert: %s, Pod: %s) to Kafka", incidentID, normalized.AlertName, normalized.PodName)
		processedCount++
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   "Alerts processed successfully",
		"processed": processedCount,
	})
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
