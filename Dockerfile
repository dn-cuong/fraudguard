# One Dockerfile for both services: docker build --build-arg CMD=worker .
FROM golang:1.26.10 AS build
ARG CMD=ingest
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
COPY config /config
ENTRYPOINT ["/app"]
