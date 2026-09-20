FROM golang:1.26-alpine AS build
WORKDIR /src
COPY contracts ./contracts
COPY services/bmkg ./services/bmkg
RUN cd services/bmkg && go build -trimpath -ldflags='-s -w' -o /out/bmkg ./cmd/server

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/bmkg /usr/local/bin/bmkg
USER app
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/bmkg"]
