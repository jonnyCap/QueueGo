# syntax=docker/dockerfile:1
FROM golang:1.24-alpine

WORKDIR /app
COPY . .

RUN go build -o queuego .

EXPOSE 9000
CMD ["./queuego"]
