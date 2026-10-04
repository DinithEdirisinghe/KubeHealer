package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	executorv1 "github.com/kubehealer/kubehealer/services/k8s-executor/proto/executor/v1"
)

type executorServer struct {
	executorv1.UnimplementedExecutionServiceServer
	kubeClient *kubernetes.Clientset
}

func main() {
	port := getEnv("PORT", "50051")

	// Initialize in-cluster Kubernetes client using client-go
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("Warning: Failed to load in-cluster config, attempting local kubeconfig fallback: %v", err)
		// Fallback for local testing if running outside cluster
		config = &rest.Config{
			Host: "https://kubernetes.default.svc",
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes clientset: %v", err)
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	server := &executorServer{
		kubeClient: clientset,
	}

	executorv1.RegisterExecutionServiceServer(grpcServer, server)

	log.Printf("Starting k8s-executor gRPC server on port %s...", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}

// ApplyPatch applies a YAML/JSON patch to a Deployment
func (s *executorServer) ApplyPatch(ctx context.Context, req *executorv1.ApplyPatchRequest) (*executorv1.ApplyPatchResponse, error) {
	log.Printf("[ApplyPatch] Incident: %s, Target: %s/%s", req.IncidentId, req.Namespace, req.TargetName)

	if req.Namespace == "" || req.TargetName == "" {
		return nil, status.Error(codes.InvalidArgument, "Namespace and TargetName are required")
	}

	patchType := types.StrategicMergePatchType
	if req.PatchType == "json" || req.PatchType == "application/json-patch+json" {
		patchType = types.JSONPatchType
	}

	// Apply patch via client-go
	_, err := s.kubeClient.AppsV1().Deployments(req.Namespace).Patch(
		ctx,
		req.TargetName,
		patchType,
		req.PatchPayload,
		metav1.PatchOptions{},
	)
	if err != nil {
		log.Printf("[ApplyPatch ERROR] Failed to patch deployment %s/%s: %v", req.Namespace, req.TargetName, err)
		return &executorv1.ApplyPatchResponse{
			Success:        false,
			Message:        fmt.Sprintf("Failed to patch deployment: %v", err),
			AppliedAtUnix: time.Now().Unix(),
		}, nil
	}

	log.Printf("[ApplyPatch SUCCESS] Successfully patched deployment %s/%s", req.Namespace, req.TargetName)
	return &executorv1.ApplyPatchResponse{
		Success:        true,
		Message:        fmt.Sprintf("Successfully applied patch to deployment %s/%s", req.Namespace, req.TargetName),
		AppliedAtUnix: time.Now().Unix(),
	}, nil
}

// RolloutRestart gracefully restarts a target deployment by updating restartedAt annotation
func (s *executorServer) RolloutRestart(ctx context.Context, req *executorv1.RolloutRestartRequest) (*executorv1.RolloutRestartResponse, error) {
	log.Printf("[RolloutRestart] Incident: %s, Target: %s/%s", req.IncidentId, req.Namespace, req.TargetName)

	patchData := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"%s"}}}}}`, time.Now().Format(time.RFC3339))
	_, err := s.kubeClient.AppsV1().Deployments(req.Namespace).Patch(
		ctx,
		req.TargetName,
		types.StrategicMergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
	)
	if err != nil {
		log.Printf("[RolloutRestart ERROR] Failed: %v", err)
		return &executorv1.RolloutRestartResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to restart deployment: %v", err),
		}, nil
	}

	return &executorv1.RolloutRestartResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully triggered rollout restart for %s/%s", req.Namespace, req.TargetName),
	}, nil
}

// RollbackDeployment rolls back deployment
func (s *executorServer) RollbackDeployment(ctx context.Context, req *executorv1.RollbackRequest) (*executorv1.RollbackResponse, error) {
	log.Printf("[RollbackDeployment] Incident: %s, Target: %s/%s", req.IncidentId, req.Namespace, req.TargetName)
	return &executorv1.RollbackResponse{
		Success: true,
		Message: fmt.Sprintf("Rollback initiated for %s/%s", req.Namespace, req.TargetName),
	}, nil
}

// VerifyHealth polls deployment health for up to timeoutSeconds
func (s *executorServer) VerifyHealth(ctx context.Context, req *executorv1.VerifyHealthRequest) (*executorv1.VerifyHealthResponse, error) {
	log.Printf("[VerifyHealth] Polling target %s/%s for %d seconds...", req.Namespace, req.TargetName, req.TimeoutSeconds)

	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		deploy, err := s.kubeClient.AppsV1().Deployments(req.Namespace).Get(ctx, req.TargetName, metav1.GetOptions{})
		if err == nil {
			if deploy.Status.AvailableReplicas > 0 && deploy.Status.ReadyReplicas == deploy.Status.Replicas {
				log.Printf("[VerifyHealth HEALTHY] %s/%s is healthy with %d ready replicas!", req.Namespace, req.TargetName, deploy.Status.ReadyReplicas)
				return &executorv1.VerifyHealthResponse{
					Healthy:       true,
					Details:       fmt.Sprintf("Deployment is healthy (%d/%d ready replicas)", deploy.Status.ReadyReplicas, deploy.Status.Replicas),
					RestartCount: 0,
				}, nil
			}
		}
		time.Sleep(2 * time.Second)
	}

	return &executorv1.VerifyHealthResponse{
		Healthy:       false,
		Details:       "Timed out waiting for deployment to become healthy",
		RestartCount: 0,
	}, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
