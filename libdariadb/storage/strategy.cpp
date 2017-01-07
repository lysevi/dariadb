#include <libdariadb/storage/strategy.h>
#include <libdariadb/utils/strings.h>
#include <libdariadb/utils/exception.h>
#include <sstream>

std::istream &dariadb::storage::operator>>(std::istream &in, STRATEGY &strat) {
  std::string token;
  in >> token;

  token = utils::strings::to_upper(token);

  if (token == "WAL") {
    strat = dariadb::storage::STRATEGY::WAL;
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
  case STRATEGY::WAL:
    stream << "WAL";
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

std::string dariadb::storage::to_string(const STRATEGY &strat) {
	std::stringstream ss;
	ss << strat;
	return ss.str();
}