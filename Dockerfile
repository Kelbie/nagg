# syntax=docker/dockerfile:1

FROM golang:1.26.3-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-migrate ./cmd/migrate
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-backfill ./cmd/backfill
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nagg-ingester ./cmd/ingester

FROM alpine:3.20

RUN apk add --no-cache ca-certificates \
  && addgroup -S nagg \
  && adduser -S -G nagg nagg

WORKDIR /app
COPY --from=build /out/ ./

USER nagg
EXPOSE 8080

CMD ["./nagg-api"]
