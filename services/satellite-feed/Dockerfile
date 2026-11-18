FROM golang:1.26-alpine AS build
WORKDIR /src
COPY services/satellite-feed ./services/satellite-feed
RUN cd services/satellite-feed && go build -trimpath -ldflags='-s -w' -o /out/satellite-feed ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/satellite-feed /usr/local/bin/satellite-feed
USER 10001:10001
EXPOSE 8088
ENTRYPOINT ["/usr/local/bin/satellite-feed"]
