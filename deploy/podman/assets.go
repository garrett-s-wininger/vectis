package podman

import _ "embed"

//go:embed kube-spec.yaml.tmpl
var KubeSpecTemplate []byte

//go:embed grafana-configmaps.gen.yaml
var GrafanaConfigMaps []byte
