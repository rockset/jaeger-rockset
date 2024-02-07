FROM golang AS builder

ENV CGO_ENABLED=0

COPY . /src
RUN cd /src && go vet ./... &&  go build -o /jaeger-rockset ./cmd/jaeger-rockset

FROM alpine

COPY --from=builder /jaeger-rockset /jaeger-rockset
CMD ["cp", "/jaeger-rockset", "/plugin/jaeger-rockset"]
