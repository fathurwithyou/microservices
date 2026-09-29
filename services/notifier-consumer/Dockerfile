FROM golang:1.26-alpine AS build
WORKDIR /src
COPY contracts ./contracts
COPY pkg ./pkg
COPY services/notifier-consumer ./services/notifier-consumer
RUN cd services/notifier-consumer && go build -trimpath -ldflags='-s -w' -o /out/notifier-consumer ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/notifier-consumer /usr/local/bin/notifier-consumer
USER 10001:10001
EXPOSE 8084
ENTRYPOINT ["/usr/local/bin/notifier-consumer"]
