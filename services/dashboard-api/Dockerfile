# Dockerfile for dashboard-api
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o dashboard-api .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /app/dashboard-api .

EXPOSE 8081
ENTRYPOINT ["./dashboard-api"]
