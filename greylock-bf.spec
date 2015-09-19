Summary:	Greylock is a massively scalable full-text searching/indexing engine
Name:		greylock
Version:	0.2.1
Release:	1%{?dist}.1

License:	GPLv2+
Group:		System Environment/Libraries
URL:		http://reverbrain.com/
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)


BuildRequires:	ribosome, elliptics-devel, elliptics-client-devel
BuildRequires:	libswarm3-devel, libthevoid3-devel
BuildRequires:	boost-devel, boost-system, boost-program-options, boost-filesystem
BuildRequires:	msgpack-devel, lz4-devel
BuildRequires:	cmake >= 2.6

%description
Greylock is a massively scalable full-text searching/indexing engine.
It is built on top of Elliptics distributed storage and its buckets (http://doc.reverbrain.com/backrunner:backrunner#bucket).
Greylock scales with elliptics storage (thousands of backends) and builds inverted indexes.
Greylock is a low-level library, package contains HTTP server built using thevoid framework, which indexes requsted json files.


%package devel
Summary: Development files for %{name}
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}


%description devel
Greylock is a massively scalable full-text searching/indexing engine.

This package contains libraries, header files and developer documentation
needed for developing software which uses greylock utils.

%prep
%setup -q

%build
export LDFLAGS="-Wl,-z,defs"
export DESTDIR="%{buildroot}"
%{cmake} .
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make install DESTDIR="%{buildroot}"

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/greylock_server
%doc conf/*.json conf/consul.d/*.json


%files devel
%defattr(-,root,root,-)
%{_includedir}/*
%{_bindir}/greylock_test
#%{_datadir}/greylock/cmake/*

%changelog
* Mon Aug 17 2015 Evgeniy Polyakov <zbr@ioremap.net> - 0.2.1
- http: reworked vector_lock, now its based on a tree of condition variables
- perf: added several indexes and seed command line arguments

* Mon Aug 17 2015 Evgeniy Polyakov <zbr@ioremap.net> - 0.2.0
- Updated index metadata
- Made read-only/read-write indexes
- Added index metadata reader
- More logs added
- Added dockerfile and configs
- Added search and perf tools
- Added packaging
- Renamed to greylock
- Added bucket selection based on weights
- Many other changes

* Sat Aug 15 2015 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.0
- Greylock is a massively scalable full-text searching/indexing engine.

