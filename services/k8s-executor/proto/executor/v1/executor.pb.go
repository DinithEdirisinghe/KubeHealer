package executorv1

import (
	context "context"
	json "encoding/json"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	encoding "google.golang.org/grpc/encoding"
	status "google.golang.org/grpc/status"
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

// ExecutionServiceClient is the client API for ExecutionService service.
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

// ExecutionServiceServer is the server API for ExecutionService service.
type ExecutionServiceServer interface {
	ApplyPatch(context.Context, *ApplyPatchRequest) (*ApplyPatchResponse, error)
	RolloutRestart(context.Context, *RolloutRestartRequest) (*RolloutRestartResponse, error)
	RollbackDeployment(context.Context, *RollbackRequest) (*RollbackResponse, error)
	VerifyHealth(context.Context, *VerifyHealthRequest) (*VerifyHealthResponse, error)
	mustEmbedUnimplementedExecutionServiceServer()
}

type UnimplementedExecutionServiceServer struct{}

func (UnimplementedExecutionServiceServer) ApplyPatch(context.Context, *ApplyPatchRequest) (*ApplyPatchResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ApplyPatch not implemented")
}
func (UnimplementedExecutionServiceServer) RolloutRestart(context.Context, *RolloutRestartRequest) (*RolloutRestartResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RolloutRestart not implemented")
}
func (UnimplementedExecutionServiceServer) RollbackDeployment(context.Context, *RollbackRequest) (*RollbackResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RollbackDeployment not implemented")
}
func (UnimplementedExecutionServiceServer) VerifyHealth(context.Context, *VerifyHealthRequest) (*VerifyHealthResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VerifyHealth not implemented")
}
func (UnimplementedExecutionServiceServer) mustEmbedUnimplementedExecutionServiceServer() {}

func RegisterExecutionServiceServer(s grpc.ServiceRegistrar, srv ExecutionServiceServer) {
	s.RegisterService(&ExecutionService_ServiceDesc, srv)
}

var ExecutionService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "executor.v1.ExecutionService",
	HandlerType: (*ExecutionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ApplyPatch",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(ApplyPatchRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(ExecutionServiceServer).ApplyPatch(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/executor.v1.ExecutionService/ApplyPatch"}
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					return srv.(ExecutionServiceServer).ApplyPatch(ctx, req.(*ApplyPatchRequest))
				}
				return interceptor(ctx, in, info, handler)
			},
		},
		{
			MethodName: "RolloutRestart",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(RolloutRestartRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(ExecutionServiceServer).RolloutRestart(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/executor.v1.ExecutionService/RolloutRestart"}
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					return srv.(ExecutionServiceServer).RolloutRestart(ctx, req.(*RolloutRestartRequest))
				}
				return interceptor(ctx, in, info, handler)
			},
		},
		{
			MethodName: "RollbackDeployment",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(RollbackRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(ExecutionServiceServer).RollbackDeployment(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/executor.v1.ExecutionService/RollbackDeployment"}
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					return srv.(ExecutionServiceServer).RollbackDeployment(ctx, req.(*RollbackRequest))
				}
				return interceptor(ctx, in, info, handler)
			},
		},
		{
			MethodName: "VerifyHealth",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(VerifyHealthRequest)
				if err := dec(in); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return srv.(ExecutionServiceServer).VerifyHealth(ctx, in)
				}
				info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/executor.v1.ExecutionService/VerifyHealth"}
				handler := func(ctx context.Context, req interface{}) (interface{}, error) {
					return srv.(ExecutionServiceServer).VerifyHealth(ctx, req.(*VerifyHealthRequest))
				}
				return interceptor(ctx, in, info, handler)
			},
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/executor.proto",
}
