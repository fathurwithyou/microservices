FROM golang:1.26-alpine AS build
WORKDIR /src
COPY pkg ./pkg
COPY services/demo-api ./services/demo-api
RUN cd services/demo-api && go build -trimpath -ldflags='-s -w' -o /out/demo-api ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/demo-api /usr/local/bin/demo-api
USER 10001:10001
EXPOSE 8086
ENTRYPOINT ["/usr/local/bin/demo-api"]
