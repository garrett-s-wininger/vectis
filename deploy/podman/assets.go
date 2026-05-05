package podman

import _ "embed"

//go:embed kube-spec.yaml
var KubeSpec []byte

//go:embed grafana-configmaps.gen.yaml
var GrafanaConfigMaps []byte
