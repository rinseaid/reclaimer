FROM golang:1.23-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /reclaimer ./cmd/reclaimer

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /reclaimer /app/reclaimer
COPY --from=builder /build/internal/web/templates /app/templates

ARG RECLAIMER_UID=1000
ARG RECLAIMER_GID=1000
RUN addgroup -S -g ${RECLAIMER_GID} reclaimer \
 && adduser -S -u ${RECLAIMER_UID} -G reclaimer -h /app -s /sbin/nologin reclaimer \
 && mkdir -p /app/data /app/data/poster-cache \
 && chown -R reclaimer:reclaimer /app

USER reclaimer

EXPOSE 8080
CMD ["/app/reclaimer"]
