FROM golang:1.26-alpine AS build
WORKDIR /src
COPY pkg ./pkg
COPY services/satellite-ingestor ./services/satellite-ingestor
RUN cd services/satellite-ingestor && go build -trimpath -ldflags='-s -w' -o /out/satellite-ingestor ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/satellite-ingestor /usr/local/bin/satellite-ingestor
USER 10001:10001
EXPOSE 8086
ENTRYPOINT ["/usr/local/bin/satellite-ingestor"]
