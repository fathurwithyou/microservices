FROM golang:1.26-alpine AS build
WORKDIR /src
COPY contracts ./contracts
COPY services/auth ./services/auth
RUN cd services/auth && go build -trimpath -ldflags='-s -w' -o /out/auth ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/auth /usr/local/bin/auth
USER app
EXPOSE 9091
ENTRYPOINT ["/usr/local/bin/auth"]
