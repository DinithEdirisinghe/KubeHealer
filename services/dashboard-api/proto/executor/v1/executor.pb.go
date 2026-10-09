package executorv1

import (
	context "context"
	json "encoding/json"

	grpc "google.golang.org/grpc"
	encoding "google.golang.org/grpc/encoding"
)

type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (JSONCodec) Name() string {
	return "json"
}

func init() {
	encoding.RegisterCodec(JSONCodec{})
}


type ApplyPatchRequest struct {
	IncidentId   string `json:"incident_id,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
	TargetKind   string `json:"target_kind,omitempty"`
	TargetName   string `json:"target_name,omitempty"`
	PatchType    string `json:"patch_type,omitempty"`
	PatchPayload []byte `json:"patch_payload,omitempty"`
}

type ApplyPatchResponse struct {
	Success       bool   `json:"success,omitempty"`
	Message       string `json:"message,omitempty"`
	AppliedAtUnix int64  `json:"applied_at_unix,omitempty"`
}

type RolloutRestartRequest struct {
	IncidentId string `json:"incident_id,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	TargetKind string `json:"target_kind,omitempty"`
	TargetName string `json:"target_name,omitempty"`
}

type RolloutRestartResponse struct {
	Success bool   `json:"success,omitempty"`
	Message string `json:"message,omitempty"`
}

type RollbackRequest struct {
	IncidentId string `json:"incident_id,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	TargetKind string `json:"target_kind,omitempty"`
	TargetName string `json:"target_name,omitempty"`
	Revision   int32  `json:"revision,omitempty"`
}

type RollbackResponse struct {
	Success bool   `json:"success,omitempty"`
	Message string `json:"message,omitempty"`
}

type VerifyHealthRequest struct {
	IncidentId     string `json:"incident_id,omitempty"`
	Namespace      string `json:"namespace,omitempty"`
	TargetName     string `json:"target_name,omitempty"`
	TimeoutSeconds int32  `json:"timeout_seconds,omitempty"`
}

type VerifyHealthResponse struct {
	Healthy      bool   `json:"healthy,omitempty"`
	Details      string `json:"details,omitempty"`
	RestartCount int32  `json:"restart_count,omitempty"`
}

type ExecutionServiceClient interface {
	ApplyPatch(ctx context.Context, in *ApplyPatchRequest, opts ...grpc.CallOption) (*ApplyPatchResponse, error)
	RolloutRestart(ctx context.Context, in *RolloutRestartRequest, opts ...grpc.CallOption) (*RolloutRestartResponse, error)
	RollbackDeployment(ctx context.Context, in *RollbackRequest, opts ...grpc.CallOption) (*RollbackResponse, error)
	VerifyHealth(ctx context.Context, in *VerifyHealthRequest, opts ...grpc.CallOption) (*VerifyHealthResponse, error)
}

type executionServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewExecutionServiceClient(cc grpc.ClientConnInterface) ExecutionServiceClient {
	return &executionServiceClient{cc}
}

func (c *executionServiceClient) ApplyPatch(ctx context.Context, in *ApplyPatchRequest, opts ...grpc.CallOption) (*ApplyPatchResponse, error) {
	out := new(ApplyPatchResponse)
	err := c.cc.Invoke(ctx, "/executor.v1.ExecutionService/ApplyPatch", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionServiceClient) RolloutRestart(ctx context.Context, in *RolloutRestartRequest, opts ...grpc.CallOption) (*RolloutRestartResponse, error) {
	out := new(RolloutRestartResponse)
	err := c.cc.Invoke(ctx, "/executor.v1.ExecutionService/RolloutRestart", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionServiceClient) RollbackDeployment(ctx context.Context, in *RollbackRequest, opts ...grpc.CallOption) (*RollbackResponse, error) {
	out := new(RollbackResponse)
	err := c.cc.Invoke(ctx, "/executor.v1.ExecutionService/RollbackDeployment", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executionServiceClient) VerifyHealth(ctx context.Context, in *VerifyHealthRequest, opts ...grpc.CallOption) (*VerifyHealthResponse, error) {
	out := new(VerifyHealthResponse)
	err := c.cc.Invoke(ctx, "/executor.v1.ExecutionService/VerifyHealth", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
