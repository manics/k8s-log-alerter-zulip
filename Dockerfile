ARG BASE_IMAGE=docker.io/library/golang:1.25-alpine
FROM --platform=$BUILDPLATFORM $BASE_IMAGE AS builder

WORKDIR /app

# Dependencies
COPY src/go.* /app/
RUN go mod download

COPY src/internal /app/internal
COPY src/*.go /app/
# Remove compiled in paths, reduce binary size by omitting
# DWARF symbol table, symbol table and debug information
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -o k8s-log-alerter-zulip -ldflags="-w -s"

######################################################################

FROM docker.io/library/alpine:latest

WORKDIR /app

COPY --from=builder /app/k8s-log-alerter-zulip .

RUN adduser -S -u1000 -H logalerter
USER logalerter
ENTRYPOINT ["/app/k8s-log-alerter-zulip"]
