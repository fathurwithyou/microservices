FROM golang:1.26-alpine AS build
WORKDIR /src
COPY contracts ./contracts
COPY services/client-api ./services/client-api
RUN cd services/client-api && go build -trimpath -ldflags='-s -w' -o /out/client-api ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/client-api /usr/local/bin/client-api
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/client-api"]
