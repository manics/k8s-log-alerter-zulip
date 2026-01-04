# K8s Log Alerter Zulip

[![Build](https://github.com/manics/k8s-log-alerter-zulip/actions/workflows/build.yml/badge.svg)](https://github.com/manics/k8s-log-alerter-zulip/actions/workflows/build.yml)

Watches Kubernetes pod logs, sends matching entries to a Zulip server.

## Configuration

See the example [`config.json`](config.json).

[Create a `Generic` Zulip bot](https://zulip.com/help/add-a-bot-or-integration) and obtain it's [email and API key](https://zulip.com/help/manage-a-bot).
Edit `site`, `bot_email`, `bot_key` and `channel` in the configuration file.

The `rules` section is a dictionary where the keys are the rule names which are used as Zulip topics, and values are:

- `pod_labels` is a dicionary of labels to be matched, all containers will be watched
- `regex` is a regular expression that matches logs that should be sent to Zulip

## Build

```sh
go build
```

## Test

```sh
go test
```

## Run

Setup your kubeconfig file and run:

```sh
./k8s-log-alerter-zulip -c config.json
```

## Docker/Podman

```sh
podman build -t k8s-log-alerter-zulip .
```

```sh
podman run --name log-watcher \
  -v ~/.kube/config:/kubeconfig:ro,z \
  -e KUBECONFIG=/kubeconfig \
  -v ./config.json:/config.json:ro,z \
  k8s-log-alerter-zulip -c /config.json
```

## Kubernetes
Create a configuration file with your Zulip connection details and at least one rule, see [`values.yaml`](helm-chart/values.yaml).

```sh
helm upgrade --install k8s-log-alerter-zulip \
  --repo=https://www.manicstreetpreacher.co.uk/k8s-log-alerter-zulip/ \
  k8s-log-alerter-zulip --values config.yaml
```
