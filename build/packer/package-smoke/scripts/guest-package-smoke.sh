profile=$1
prep_version=$2

require_sudo "the package smoke VM"

case "$profile" in
	deb)
		install_apt_packages "deb package smoke profile requires an apt-based guest" ca-certificates dpkg systemd
		require_commands dpkg dpkg-deb
		;;
	rpm)
		install_rpm_packages "rpm package smoke profile requires dnf or microdnf" ca-certificates rpm systemd
		require_commands rpm
		;;
esac

require_commands systemctl systemd-sysusers systemd-tmpfiles

write_prep_marker /etc/vectis-vm-prep/package-smoke-profile "$profile"
write_prep_marker /etc/vectis-vm-prep/package-smoke-prep-version "$prep_version"
