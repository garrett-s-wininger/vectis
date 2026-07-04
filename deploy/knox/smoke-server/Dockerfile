FROM docker.io/library/golang:1.25.10-alpine AS build

WORKDIR /src
COPY artifacts/integrations/knox/src/ ./
COPY deploy/knox/smoke-server/main.go.tmpl ./cmd/vectis_smoke_server/main.go
RUN go build -tags knox_smoke_server -o /out/knox-smoke-server ./cmd/vectis_smoke_server

FROM docker.io/library/alpine:3.23

COPY --from=build /out/knox-smoke-server /usr/local/bin/knox-smoke-server
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/knox-smoke-server"]
