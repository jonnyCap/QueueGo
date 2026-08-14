# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o queuego ./cmd/queuego

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/queuego /app/queuego
COPY configs/ /app/configs/

EXPOSE 9000
CMD ["./queuego"]
