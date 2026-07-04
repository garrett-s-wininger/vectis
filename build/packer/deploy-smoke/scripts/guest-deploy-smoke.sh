prep_version=$1

require_sudo "the deploy smoke VM"
install_apt_packages "deploy smoke profile requires an apt-based guest" ca-certificates systemd
require_commands systemctl systemd-analyze systemd-sysusers systemd-tmpfiles

write_prep_marker /etc/vectis-vm-prep/deploy-smoke-profile systemd
write_prep_marker /etc/vectis-vm-prep/deploy-smoke-prep-version "$prep_version"
