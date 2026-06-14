packer {
  required_version = ">= 1.10.0"
}

variable "instance" {
  type    = string
  default = "vectis-package-builder"
}

variable "base_template" {
  type    = string
  default = "ubuntu-lts"
}

variable "go_version" {
  type = string
}

variable "go_sha256" {
  type    = string
  default = ""
}

variable "cpus" {
  type    = number
  default = 4
}

variable "memory" {
  type    = number
  default = 4
}

variable "disk" {
  type    = number
  default = 60
}

variable "stop_after_prepare" {
  type    = bool
  default = true
}

variable "lima_bin" {
  type    = string
  default = "limactl"
}

variable "workspace_root" {
  type    = string
  default = "/var/tmp/vectis-package-local-workspaces"
}

variable "cache_root" {
  type    = string
  default = "/var/tmp/vectis-package-local-cache"
}

source "null" "lima" {
  communicator = "none"
}

build {
  name    = "vectis-package-builder"
  sources = ["source.null.lima"]

  provisioner "shell-local" {
    environment_vars = [
      "VECTIS_PACKER_LIMA_BIN=${var.lima_bin}",
      "VECTIS_PACKER_LIMA_INSTANCE=${var.instance}",
      "VECTIS_PACKER_LIMA_TEMPLATE=${var.base_template}",
      "VECTIS_PACKER_GO_VERSION=${var.go_version}",
      "VECTIS_PACKER_GO_SHA256=${var.go_sha256}",
      "VECTIS_PACKER_CPUS=${var.cpus}",
      "VECTIS_PACKER_MEMORY=${var.memory}",
      "VECTIS_PACKER_DISK=${var.disk}",
      "VECTIS_PACKER_STOP_AFTER_PREPARE=${var.stop_after_prepare}",
      "VECTIS_PACKER_WORKSPACE_ROOT=${var.workspace_root}",
      "VECTIS_PACKER_CACHE_ROOT=${var.cache_root}",
    ]

    inline = ["sh ${path.root}/scripts/prepare-lima-package-builder.sh"]
  }
}
