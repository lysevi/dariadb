#include <libdariadb/storage/strategy.h>
#include <libdariadb/utils/strings.h>
#include <libdariadb/utils/exception.h>

std::istream &dariadb::storage::operator>>(std::istream &in, STRATEGY &strat) {
  std::string token;
  in >> token;

  token = utils::strings::to_upper(token);

  if (token == "FAST_WRITE") {
    strat = dariadb::storage::STRATEGY::FAST_WRITE;
	return in;
  }
  if (token == "COMPRESSED") {
    strat = dariadb::storage::STRATEGY::COMPRESSED;
	return in;
  }

  if (token == "MEMORY") {
    strat = dariadb::storage::STRATEGY::MEMORY;
	return in;
  }

  if (token == "CACHE") {
    strat = dariadb::storage::STRATEGY::CACHE;
	return in;
  }
  THROW_EXCEPTION("engine: bad strategy name - ", token);
}

std::ostream &dariadb::storage::operator<<(std::ostream &stream, const STRATEGY &strat) {
  switch (strat) {
  case STRATEGY::COMPRESSED:
    stream << "COMPRESSED";
    break;
  case STRATEGY::FAST_WRITE:
    stream << "FAST_WRITE";
    break;
  case STRATEGY::MEMORY:
    stream << "MEMORY";
    break;
  case STRATEGY::CACHE:
    stream << "CACHE";
    break;
  default:
	THROW_EXCEPTION("engine: bad strategy - ", (uint16_t)strat);
    break;
  };
  return stream;
}
