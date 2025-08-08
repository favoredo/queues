FROM golang:1.24 as builder

WORKDIR /app
COPY go.mod .
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o consumer .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/consumer .
CMD ["./consumer"]