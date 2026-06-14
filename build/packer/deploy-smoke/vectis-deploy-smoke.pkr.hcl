packer {
  required_version = ">= 1.10.0"
}

variable "instance" {
  type = string
}

variable "base_template" {
  type = string
}

variable "cpus" {
  type    = number
  default = 2
}

variable "memory" {
  type    = number
  default = 2
}

variable "disk" {
  type    = number
  default = 30
}

variable "stop_after_prepare" {
  type    = bool
  default = true
}

variable "lima_bin" {
  type    = string
  default = "limactl"
}

source "null" "lima" {
  communicator = "none"
}

build {
  name    = "vectis-deploy-smoke"
  sources = ["source.null.lima"]

  provisioner "shell-local" {
    environment_vars = [
      "VECTIS_PACKER_LIMA_BIN=${var.lima_bin}",
      "VECTIS_PACKER_LIMA_INSTANCE=${var.instance}",
      "VECTIS_PACKER_LIMA_TEMPLATE=${var.base_template}",
      "VECTIS_PACKER_CPUS=${var.cpus}",
      "VECTIS_PACKER_MEMORY=${var.memory}",
      "VECTIS_PACKER_DISK=${var.disk}",
      "VECTIS_PACKER_STOP_AFTER_PREPARE=${var.stop_after_prepare}",
    ]

    inline = ["sh ${path.root}/scripts/prepare-lima-deploy-smoke.sh"]
  }
}
