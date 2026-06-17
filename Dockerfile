# syntax=docker/dockerfile:1

FROM golang:1.26.3-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-migrate ./cmd/migrate
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-backfill ./cmd/backfill
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-retention ./cmd/retention
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-ingester ./cmd/ingester
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-enricher ./cmd/enricher

FROM alpine:3.20

RUN apk add --no-cache ca-certificates \
  && addgroup -S nagg \
  && adduser -S -G nagg nagg

WORKDIR /app
COPY --from=build /out/ ./

USER nagg
EXPOSE 8080

CMD ["sh", "-c", "case \"${NAGG_PROCESS:-api}\" in api) exec ./nagg-api ;; ingester) exec ./nagg-ingester ;; enricher) exec ./nagg-enricher ;; migrate) exec ./nagg-migrate ;; backfill) exec ./nagg-backfill ;; retention) exec ./nagg-retention ;; *) echo \"unknown NAGG_PROCESS=${NAGG_PROCESS}\" >&2; exit 1 ;; esac"]
