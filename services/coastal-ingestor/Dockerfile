FROM golang:1.26-alpine AS build
WORKDIR /src
COPY pkg ./pkg
COPY services/coastal-ingestor ./services/coastal-ingestor
RUN cd services/coastal-ingestor && go build -trimpath -ldflags='-s -w' -o /out/coastal-ingestor ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/coastal-ingestor /usr/local/bin/coastal-ingestor
USER 10001:10001
EXPOSE 8087
ENTRYPOINT ["/usr/local/bin/coastal-ingestor"]
