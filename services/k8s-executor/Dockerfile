# Dockerfile for k8s-executor
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o k8s-executor .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /app/k8s-executor .

EXPOSE 50051
ENTRYPOINT ["./k8s-executor"]
