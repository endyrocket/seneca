# Seneca Helm Chart

A Helm chart for deploying the Seneca GPU XID Root Cause Analysis Service to Kubernetes.

## Overview

This service provides automated root cause analysis for NVIDIA GPU XID errors in Kubernetes clusters. It:

- Receives Alertmanager webhook notifications about GPU XID errors
- Fetches relevant system logs using privileged pods
- Performs AI-powered root cause analysis using OpenAI
- Checks GPU availability via Prometheus metrics
- Optionally auto-cordons nodes based on confidence levels
- Sends comprehensive analysis reports to Slack

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- Prometheus server for GPU metrics
- OpenAI API key
- Slack webhook URL

## Installation

1. **Add secrets** (recommended to use external-secrets or similar):
   ```bash
   helm install seneca ./helm-chart \
     --set secrets.openaiApiKey="sk-your-openai-key" \
     --set secrets.slackWebhookUrl="https://hooks.slack.com/services/your/webhook/url"
   ```

2. **Configure Prometheus URL**:
   ```bash
   helm install seneca ./helm-chart/seneca \
     --set config.prometheusUrl="http://your-prometheus-server:9090"
   ```

3. **Custom values file**:
   ```bash
   helm install seneca ./helm-chart -f values-prod.yaml
   ```

## Configuration

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `config.enableAutoCordon` | Enable automatic node cordoning | `true` |
| `config.autoCordonConfidenceThreshold` | Confidence threshold for auto-cordoning (%) | `80` |
| `config.openaiModel` | OpenAI model to use | `"gpt-4o"` |
| `config.prometheusUrl` | Prometheus server URL | `"http://prometheus-server:80"` |
| `config.skipLogFetching` | Skip log fetching (for debugging) | `false` |
| `secrets.openaiApiKey` | OpenAI API key | `""` |
| `secrets.slackWebhookUrl` | Slack webhook URL | `""` |

### RBAC Permissions

The service requires cluster-level permissions for:
- **Pods**: Create, read, delete (for log fetching)
- **Pods/exec**: Create (for executing commands in log pods)
- **Nodes**: Read, patch (for GPU status and cordoning)
- **Events**: Create (for audit trail)

## Usage

### Webhook Endpoint

The service exposes a webhook at `/webhook` that accepts Alertmanager notifications:

```yaml
# alertmanager.yml
route:
  receiver: seneca-rca

receivers:
- name: seneca-rca
  webhook_configs:
  - url: http://seneca/webhook
```

### Health Check

Health endpoint available at `/health` for monitoring:

```bash
curl http://seneca/health
```

### Metrics

Prometheus metrics exposed at `/metrics`:
- RCA processing duration
- Alert queue metrics
- Cache hit/miss rates
- Log fetching performance

## Security Considerations

1. **Secrets Management**: Use external-secrets or similar instead of plain text
2. **RBAC**: The service requires privileged permissions - review carefully
3. **Pod Security**: Review security contexts based on your requirements

## Troubleshooting

### Common Issues

1. **Log fetching fails**: Check if privileged pods are allowed in your cluster
2. **GPU metrics unavailable**: Verify Prometheus URL and GPU metrics availability
3. **Cordoning fails**: Check RBAC permissions for node patching
4. **OpenAI API errors**: Verify API key and model availability

## Uninstallation

```bash
helm uninstall seneca
```

Note: This will also remove the ClusterRole and ClusterRoleBinding. 