Summary:	Greylock is a massively scalable full-text searching/indexing engine
Name:		greylock
Version:	0.1.0
Release:	1%{?dist}.1

License:	GPLv2+
Group:		System Environment/Libraries
URL:		http://reverbrain.com/
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)


BuildRequires:	ribosome, elliptics-devel, elliptics-client
BuildRequires:	libswarm3-devel, libthevoid3-devel
BuildRequires:	boost-devel, boost-system, boost-program-options
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


%files devel
%defattr(-,root,root,-)
%{_includedir}/*
%{_bindir}/greylock_test
#%{_datadir}/greylock/cmake/*

%changelog
* Sat Aug 15 2015 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.0
- Greylock is a massively scalable full-text searching/indexing engine.

