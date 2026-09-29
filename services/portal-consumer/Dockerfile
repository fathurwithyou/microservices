FROM golang:1.26-alpine AS build
WORKDIR /src
COPY contracts ./contracts
COPY pkg ./pkg
COPY services/portal-consumer ./services/portal-consumer
RUN cd services/portal-consumer && go build -trimpath -ldflags='-s -w' -o /out/portal-consumer ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/portal-consumer /usr/local/bin/portal-consumer
USER 10001:10001
EXPOSE 8085
ENTRYPOINT ["/usr/local/bin/portal-consumer"]
