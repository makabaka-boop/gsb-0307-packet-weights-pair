# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/packets .

FROM alpine:3.21
COPY --from=build /out/packets /usr/local/bin/packets
ENTRYPOINT ["packets"]
