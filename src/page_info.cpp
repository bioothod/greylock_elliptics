#include <fstream>
#include <iostream>

#include "greylock/bucket_transport.hpp"
#include "greylock/intersection.hpp"

#include <boost/program_options.hpp>

using namespace ioremap;

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	std::vector<std::string> remotes;

	bpo::options_description generic("Page info reader options");
	generic.add_options()
		("help", "this help message")
		;


	std::string log_file, log_level, metagroups;
	bpo::options_description ell("Elliptics options");
	ell.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->composing(), "remote node: addr:port:family")
		("log-file", bpo::value<std::string>(&log_file)->default_value("/dev/stdout"), "log file")
		("log-level", bpo::value<std::string>(&log_level)->default_value("error"), "log level: error, info, notice, debug")
		("metagroups", bpo::value<std::string>(&metagroups), "metadata groups where bucket info is stored: 1:2:3")
		;

	std::string key_name, key_file;
	bpo::options_description gr("Page options");
	gr.add_options()
		("key", bpo::value<std::string>(&key_name),
			"page key as hex string (128 bytes long): 00aabbccdd1122...")
		("key-file", bpo::value<std::string>(&key_file), "file where page data lives")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic).add(ell).add(gr);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << cmdline_options << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << cmdline_options << std::endl;
		return -1;
	}

	try {
		greylock::page p;

		if (!key_file.size()) {
			if (remotes.empty()) {
				std::cerr << "You must provide remote node\n" << cmdline_options << std::endl;
				return -1;
			}

			if (metagroups.empty()) {
				std::cerr << "You must provide metadata groups\n" << cmdline_options << std::endl;
				return -1;
			}

			if (key_name.empty()) {
				std::cerr << "You must provide remote key\n" << cmdline_options << std::endl;
				return -1;
			}
#if 0
			greylock::elliptics_transport t(log_file, log_level);
			t.add_remotes(remotes);

			greylock::bucket_transport bt(t.get_node());
			if (!bt.init(elliptics::parse_groups(metagroups.c_str()), bnames)) {
				std::cerr << "Could not initialize bucket transport, exiting";
				return -1;
			}

			greylock::eurl start;
			start.key = iname;
			start.bucket = bnames[0];

			greylock::read_only_index<greylock::bucket_transport> idx(bt, start);
			std::cout << idx.meta().str() << std::endl;
#endif
		} else {
			std::ifstream in(key_file.c_str());
			std::ostringstream ss;
			ss << in.rdbuf();

			std::string content = ss.str();
			p.load(content.data(), content.size());
		}

		std::cout << "page: " << p.str() << std::endl;
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}

