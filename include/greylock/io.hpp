#pragma once

#include "greylock/core.hpp"

#include <ebucket/bucket_processor.hpp>

namespace ioremap { namespace greylock {

struct io {
	static elliptics::async_read_result read_data(ebucket::bucket_processor &bp, const eurl &url, bool read_latest) {
		ebucket::bucket b;
		auto err = bp.find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_read_result result(bp.error_session());
			elliptics::async_result_handler<elliptics::read_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		elliptics::async_read_result async;
		elliptics::session s = b->session();

		s.set_filter(elliptics::filters::positive);

		if (read_latest) {
			async = s.read_latest(url.key, 0, 0);
		} else {
			async = s.read_data(url.key, 0, 0);
		}

		return async;
	}

	static elliptics::async_lookup_result prepare_latest(ebucket::bucket_processor &bp, const eurl &url) {
		ebucket::bucket b;

		elliptics::error_info err = bp.find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_lookup_result result(bp.error_session());
			elliptics::async_result_handler<elliptics::lookup_result_entry> handler(result);
			handler.complete(err);

			return result;
		}

		return b->session().prepare_latest(url.key, b->meta().groups);
	}

	static elliptics::async_write_result write(ebucket::bucket_processor &bp, const eurl &url,
			const std::string &data, size_t reserve_size, bool cache) {
		ebucket::bucket b;
		elliptics::error_info err = bp.find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_write_result result(bp.error_session());
			elliptics::async_result_handler<elliptics::write_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		elliptics::data_pointer dp = elliptics::data_pointer::from_raw((char *)data.data(), data.size());

		elliptics::session s = b->session();
		if (cache) {
			s.set_ioflags(DNET_IO_FLAGS_CACHE);
		}
		s.set_filter(elliptics::filters::all);

		elliptics::key id(url.key);
		s.transform(id);

		dnet_io_control ctl;

		memset(&ctl, 0, sizeof(ctl));
		dnet_current_time(&ctl.io.timestamp);

		ctl.cflags = s.get_cflags();
		ctl.data = dp.data();

		ctl.io.flags = s.get_ioflags() | DNET_IO_FLAGS_PREPARE | DNET_IO_FLAGS_PLAIN_WRITE | DNET_IO_FLAGS_COMMIT;
		ctl.io.user_flags = s.get_user_flags();
		ctl.io.offset = 0;
		ctl.io.size = dp.size();
		ctl.io.num = reserve_size;
		if (ctl.io.size > ctl.io.num) {
			ctl.io.num = ctl.io.size * 1.5;
		}

		ctl.id = id.id();
		ctl.fd = -1;

		BH_LOG(bp.logger(), DNET_LOG_NOTICE,
				"%s: bucket write: bucket: %s, key: %s, data-size: %d, reserve-size: %d, cache: %d, ts: %s (%ld.%ld)\n",
				dnet_dump_id(&ctl.id),
				b->meta().name.c_str(), url.key.c_str(), data.size(), reserve_size, cache,
				dnet_print_time(&ctl.io.timestamp), (long)ctl.io.timestamp.tsec, (long)ctl.io.timestamp.tnsec);

		return s.write_data(ctl);
	}

	static elliptics::async_remove_result remove(ebucket::bucket_processor &bp, const eurl &url) {
		ebucket::bucket b;
		elliptics::error_info err = bp.find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_remove_result result(bp.error_session());
			elliptics::async_result_handler<elliptics::remove_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		return b->session().remove(url.key);
	}

	static std::string generate(ebucket::bucket_processor &bp, const std::string &bname, const std::string &key) {
		elliptics::session s(bp.error_session());
		s.set_namespace(bname.data(), bname.size());
		elliptics::key k(key);
		s.transform(k);

		DNET_DUMP_ID_LEN(name, &k.id(), DNET_ID_SIZE);
		return key + "." + std::string(name);
	}
};

}} // namespace ioremap::greylock
