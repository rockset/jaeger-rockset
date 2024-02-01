# Rockset Jaeger Span Storage Plugin

This is a [gRPC storage plugin]() for Jaeger that stores spans in Rockset.

## Kubernetes Deployment

Configmap for Rockset plugin

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: jaeger-rockset
  labels:
    jaeger-rockset: test
data:
  config.yaml: |
    apiserver: api.usw2a1.rockset.com
    apikey: ...
    config:
      workspace: tracing
      spans: spans
      operations: operations
  EOF
```

```yaml
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger-rockset
  labels:
    jaeger-rockset: test
spec:
  storage:
    type: grpc-plugin
    grpcPlugin:
      image: rockset/jaeger-rockset:latest
    options:
      grpc-storage-plugin:
        binary: /plugin/jaeger-rockset
        configuration-file: /plugin-config/config.yaml
        log-level: debug
  volumeMounts:
    - name: plugin-config
      mountPath: /plugin-config
  volumes:
    - name: plugin-config
      configMap:
        name: jaeger-rockset
```
