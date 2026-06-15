package kubernetes

import _ "embed"

//go:embed manifests.yaml.tmpl
var embeddedManifestTemplate []byte
