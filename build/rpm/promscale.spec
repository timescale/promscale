# RPM specfile for Promscale
# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE for a copy of the license.

Name            : promscale
Version         : _VERSION_
Release         : 1%{?dist}
Group           : Network Servers
License         : Apache License 2.0
Source          : https://github.com/timescale/%{name}/archive/%{version}.tar.gz#/%{name}-%{version}.tgz
URL             : https://github.com/timescale/%{name}
Summary         : An analytical platform and long-term store for Prometheus
BuildRequires   : systemd-rpm-macros
Requires(pre)   : shadow-utils
Requires(postun): shadow-utils


%description
Promscale is a horizontally scalable and operationally mature analytical
platform for Prometheus data that offers the combined power of PromQL
and SQL, enabling developers to ask any question, create any dashboard,
and achieve greater visibility into their systems.  Promscale is built
on top of TimescaleDB, the relational database for time-series built on
top of PostgreSQL.

%define debug_package %{nil}

%undefine __brp_mangle_shebangs
%undefine __brp_ldconfig

%prep
%setup -q

%build
echo "Build step. Nothing to do"

%install
install -d %{buildroot}%{_localstatedir}
install -d %{buildroot}%{_localstatedir}/empty
install -d %{buildroot}%{_localstatedir}/empty/%{name}
install -d %{buildroot}%{_sbindir}
install -d %{buildroot}%{_sysconfdir}
install -d %{buildroot}%{_sysconfdir}/sysconfig
install -d %{buildroot}%{_unitdir}

install -t %{buildroot}%{_sbindir} %{name}
cat > %{buildroot}%{_unitdir}/%{name}.service <<- EOT
[Unit]
Description=TimescaleDB Promscale Service
Documentation=https://github.com/timescale/%{name}
After=syslog.target
After=network.target

[Service]
Type=simple
User=%{name}
Group=%{name}
EnvironmentFile=-%{_sysconfdir}/sysconfig/%{name}
ExecStart=/usr/sbin/%{name} \$OPTIONS
Restart=on-failure
RestartSec=10
KillMode=mixed
KillSignal=SIGINT
ProtectSystem=strict
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOT

cat > %{buildroot}%{_sysconfdir}/sysconfig/%{name} <<- EOT
# PROMSCALE_ASYNC_ACKS=""
# PROMSCALE_DB_CONNECT_RETRIES=""
# PROMSCALE_DB_CONNECTIONS_MAX="-1"
# PROMSCALE_DB_HOST="localhost"
# PROMSCALE_DB_NAME="timescale"
# PROMSCALE_DB_PASSWORD=""
# PROMSCALE_DB_PORT="5432"
PROMSCALE_DB_SSL_MODE="prefer"
# PROMSCALE_DB_USER="postgres"
# PROMSCALE_DB_WRITER_CONNECTION_CONCURRENCY="4"
# PROMSCALE_INSTALL_EXTENSIONS="true"
# PROMSCALE_LABELS_CACHE_SIZE="10000"
# PROMSCALE_LEADER_ELECTION_PG_ADVISORY_LOCK_ID=""
# PROMSCALE_LEADER_ELECTION_PG_ADVISORY_LOCK_PROMETHEUS_TIMEOUT="-1ns"
# PROMSCALE_LEADER_ELECTION_REST=""
# PROMSCALE_LOG_FORMAT="logfmt"
# PROMSCALE_METRICS_CACHE_SIZE="10000"
# PROMSCALE_MIGRATE="true"
# PROMSCALE_SCHEDULED_ELECTION_INTERVAL="5s"
# PROMSCALE_TPUT_REPORT=""
# PROMSCALE_USE_SCHEMA_VERSION_LEASE=""
# PROMSCALE_WEB_CORS_ORIGIN=".*"
# PROMSCALE_WEB_LISTEN_ADDRESS=":9201"
# PROMSCALE_WEB_TELEMETRY_PATH="/metrics"

# OPTIONS=""
EOT

%files
%defattr(-,root,root,-)
%attr(0755,root,root) %{_sbindir}/%{name}
%attr(0644,root,root) %{_unitdir}/%{name}.service
%attr(0644,root,root) %config(noreplace) %{_sysconfdir}/sysconfig/%{name}
%attr(0755,%{name},%{name}) %dir %{_localstatedir}/empty/%{name}

%pre
getent passwd %{name} >/dev/null || useradd -Mrd %{_localstatedir}/empty/%{name} -s /sbin/nologin %{name}

%post
%systemd_post %{name}.service
echo "Promscale installed as a systemd service"
echo "Enable auto-start with: systemctl enable %{name}"
echo "And start the service now with: systemctl start %{name}"
echo "----------------------------------------"
echo "Modify configuration by editing %{_sysconfdir}/sysconfig/%{name}"
echo "And then restart the service with: systemctl restart %{name}"

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun %{name}.service
getent passwd %{name} >/dev/null && userdel %{name} 2>/dev/null

%changelog
* Tue Nov 13 2020 Blagoj Atanasovski <blagoj@timescale.com> - 0.1.2
- Integrate packaging with promscale goreleaser config
* Tue Nov 3 2020 Matouš Jan Fialka <mjf@mjf.cz> - 0.1.1-1
- Initial packaging
# vi:ft=spec:tw=72:nowrap
