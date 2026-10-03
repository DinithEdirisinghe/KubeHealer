# Dockerfile for alert-gateway
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o alert-gateway .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /app/alert-gateway .

EXPOSE 8080
ENTRYPOINT ["./alert-gateway"]
