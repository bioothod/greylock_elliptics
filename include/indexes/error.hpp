#ifndef __INDEXES_ERROR_HPP
#define __INDEXES_ERROR_HPP

#include <elliptics/session.hpp>

namespace ioremap { namespace indexes {

struct status {
	int				group;			// group this status came from

	int				error = 0;		// negative error code
	std::string			message;		// error message

	elliptics::data_pointer		data;			// associated data

	status() {}

	status(const elliptics::callback_result_entry &ent) {
		if (!ent.is_valid()) {
			error = -EINVAL;
			message = "invalid callback result entry";
		} else if (ent.error()) {
			error = ent.error().code();
			message = ent.error().message();
		} else {
			group = ent.command()->id.group_id;
		}
	}
	status(const elliptics::read_result_entry &ent) {
		if (!ent.is_valid()) {
			error = -EINVAL;
			message = "invalid read result entry";
		} else if (ent.error()) {
			error = ent.error().code();
			message = ent.error().message();
		} else {
			data = ent.file();
			group = ent.command()->id.group_id;
		}
	}
};

}} // namespace ioremap::indexes

#endif // __INDEXES_ERROR_HPP
