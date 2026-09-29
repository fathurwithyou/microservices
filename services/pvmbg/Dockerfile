FROM golang:1.26-alpine AS build
WORKDIR /src
COPY pkg ./pkg
COPY services/pvmbg ./services/pvmbg
RUN cd services/pvmbg && go build -trimpath -ldflags='-s -w' -o /out/pvmbg ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/pvmbg /usr/local/bin/pvmbg
USER 10001:10001
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/pvmbg"]
