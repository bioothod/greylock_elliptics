Summary:	Greylock is a massively scalable full-text searching/indexing engine
Name:		greylock
Version:	0.3.1
Release:	1%{?dist}.1

License:	GPLv2+
Group:		System Environment/Libraries
URL:		http://reverbrain.com/
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)


BuildRequires:	ribosome-devel, ebucket, elliptics-devel, elliptics-client-devel
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
%{_bindir}/greylock_meta
%{_bindir}/greylock_page_info
%doc conf/


%files devel
%defattr(-,root,root,-)
%{_includedir}/*
%{_datadir}/greylock/cmake/*

%changelog
* Sun May 22 2016 Evgeniy Polyakov <zbr@ioremap.net> - 0.3.1
- io: when printing bucket write debug info, use correct url.str()
- index: set 'modified' to given index when creating its start page
- index: fixed spelling

* Sat May 21 2016 Evgeniy Polyakov <zbr@ioremap.net> - 0.3.0
- index: switched to new page/key generation scheme, which includes null-byte in the names of internal objects stored in elliptics
- cmake: added greylock config cmake file
- Switched to ebucket project instead of internal bucket implementation. Fixed bug in remove() method (id/url update)
- page: added iterator constructor which allows to use read_latest() instead of read()
- index: added read-only index, moved index recovery into own function
- index_directory: aded search query
- server: use ribosome::vector_lock
- Major refactoring. Switched to elliptics API instead of additional abstraction level for proper error handling and performance.
- Added tool to index directory of go/cpp/hpp/c/h files or list of files. Tool performs 'dnet_usage main' search after each document has been indexed, this is needed to test monotonically increasing (or constant) number of returned search result.
- Added tool to read and unpack page content
- index: when updating page id, it should also update timestamp, so copy the whole key and change url if needed
- spec: depend on ribosome-devel
- spec: added elliptics-client-devel package
- index: switched to new index format (with positions support) and new request/reply format
- search: removed search client, use client.py instead
- Switched to new indexing/searching HTTP API described in src/{index,search}.json files. It naturally supports attributes. So far only tex
- t attributes are supported.
- Added sec.nsec timestamps into indexes. We put it into string index, but there should also be numerical indexes. That's yet a to-be-imple
- mented feature.

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

