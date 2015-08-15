FROM ubuntu:trusty

RUN	echo "deb http://repo.reverbrain.com/trusty/ current/amd64/" > /etc/apt/sources.list.d/reverbrain.list && \
	echo "deb http://repo.reverbrain.com/trusty/ current/all/" >> /etc/apt/sources.list.d/reverbrain.list && \
	apt-get install -y curl tzdata && \
	cp -f /usr/share/zoneinfo/posix/W-SU /etc/localtime && \
	curl http://repo.reverbrain.com/REVERBRAIN.GPG | apt-key add - && \
	apt-get update && \
	apt-get upgrade -y && \
	apt-get install -y git elliptics-client elliptics-dev g++ && \
	apt-get install -y cmake debhelper cdbs devscripts && \
	git config --global user.email "zbr@ioremap.net" && \
	git config --global user.name "Evgeniy Polyakov"

RUN	git clone https://github.com/bioothod/lz4 && \
	cd lz4 && \
	git branch -v && \
	debuild -sa; \
	cd - && \
	dpkg -i liblz4*_amd64.deb && \
	echo "Proper LZ4 packages (including framing support) have been installed"

RUN	apt-get install -y libicu-dev libeigen3-dev && \
	git clone https://github.com/reverbrain/ribosome && \
	cd ribosome && \
	git branch -v && \
	debuild -sa; \
	cd - && \
	dpkg -i ribosome_*_amd64.deb && \
	echo "Ribosome package has been updated and installed"

RUN	apt-get install -y libboost-system-dev libboost-filesystem-dev libboost-program-options-dev libmsgpack-dev libswarm3-dev libthevoid3-dev && \
	git clone https://github.com/reverbrain/greylock && \
	cd greylock && \
	git branch -v && \
	debuild -sa; \
	cd - && \
	dpkg -i greylock_*_amd64.deb && \
	echo "Greylock package has been updated and installed" && \
    	rm -rf /var/lib/apt/lists/*

EXPOSE 8080 21235

