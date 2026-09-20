FROM golang:1.26-alpine AS build
WORKDIR /src
COPY contracts ./contracts
COPY services/aggregator ./services/aggregator
RUN cd services/aggregator && go build -trimpath -ldflags='-s -w' -o /out/aggregator ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/aggregator /usr/local/bin/aggregator
USER app
EXPOSE 8090 9090
ENTRYPOINT ["/usr/local/bin/aggregator"]
