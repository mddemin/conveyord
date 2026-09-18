# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/conveyord ./cmd/conveyord \
 && CGO_ENABLED=0 go build -o /out/agent ./cmd/agent

FROM alpine:3.21 AS conveyord
RUN apk add --no-cache ca-certificates
COPY --from=build /out/conveyord /usr/local/bin/conveyord
ENTRYPOINT ["conveyord"]

FROM alpine:3.21 AS agent
RUN apk add --no-cache ca-certificates
COPY --from=build /out/agent /usr/local/bin/agent
ENTRYPOINT ["agent"]
