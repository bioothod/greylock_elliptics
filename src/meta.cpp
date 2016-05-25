#include <iostream>

#include "greylock/intersection.hpp"

#include <ebucket/bucket_processor.hpp>
#include <boost/program_options.hpp>

#include <ribosome/timer.hpp>

using namespace ioremap;

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	std::vector<std::string> remotes;


	bpo::options_description generic("Index metadata reader options");
	generic.add_options()
		("help", "this help message")
		;


	std::string log_file, log_level, metagroups;
	bpo::options_description ell("Elliptics options");
	ell.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->required()->composing(), "remote node: addr:port:family")
		("log-file", bpo::value<std::string>(&log_file)->default_value("/dev/stdout"), "log file")
		("log-level", bpo::value<std::string>(&log_level)->default_value("error"), "log level: error, info, notice, debug")
		("metagroups", bpo::value<std::string>(&metagroups)->required(), "metadata groups where bucket info is stored: 1:2:3")
		;

	std::vector<std::string> bnames;
	std::string iname;
	bpo::options_description gr("Greylock index options");
	gr.add_options()
		("index", bpo::value<std::string>(&iname)->required(), "index name")
		("bucket", bpo::value<std::vector<std::string>>(&bnames)->composing()->required(), "index start page lives in this bucket")
		("dump", "dump index keys to stdout")
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
		elliptics::file_logger log(log_file.c_str(), elliptics::file_logger::parse_level(log_level));
		std::shared_ptr<elliptics::node> node(new elliptics::node(elliptics::logger(log, blackhole::log::attributes_t())));

		std::vector<elliptics::address> rem(remotes.begin(), remotes.end());
		node->add_remote(rem);

		ebucket::bucket_processor bp(node);
		if (!bp.init(elliptics::parse_groups(metagroups.c_str()), bnames)) {
			std::cerr << "Could not initialize bucket transport, exiting";
			return -1;
		}

		greylock::eurl start;
		start.key = iname;
		start.bucket = bnames[0];

		greylock::read_only_index idx(bp, start);
		std::cout << idx.meta().str() << std::endl;

		if (vm.count("dump")) {
			for (auto it = idx.begin(), end = idx.end(); it != end; ++it) {
				dnet_time dt;
				it->get_timestamp((long *)&dt.tsec, (long *)&dt.tnsec);

				std::cout << "url: " << it->url.str() <<
					", id: " << it->id <<
					", timestamp: " << dnet_print_time(&dt) <<
					std::endl;
			}
		}
	} catch (const std::exception &e) {
		std::cerr << "Exception: " << e.what() << std::endl;
	}
}
