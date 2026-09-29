FROM golang:1.26-alpine AS build
WORKDIR /src
COPY contracts ./contracts
COPY pkg ./pkg
COPY services/dashboard-consumer ./services/dashboard-consumer
RUN cd services/dashboard-consumer && go build -trimpath -ldflags='-s -w' -o /out/dashboard-consumer ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/dashboard-consumer /usr/local/bin/dashboard-consumer
USER 10001:10001
EXPOSE 8083
ENTRYPOINT ["/usr/local/bin/dashboard-consumer"]
