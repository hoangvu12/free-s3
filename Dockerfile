FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/free-s3 ./cmd/free-s3

FROM alpine:3.22

# CA certs so the provider HTTPS uploads/downloads validate.
RUN apk add --no-cache ca-certificates && adduser -D -h /app appuser
WORKDIR /app
COPY --from=build /out/free-s3 /usr/local/bin/free-s3

ENV LISTEN_ADDR=:9000 \
    DATABASE_PATH=/app/data/free-s3.db
EXPOSE 9000
VOLUME ["/app/data"]

USER appuser
CMD ["free-s3"]
