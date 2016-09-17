#include "strategy.h"
#include "../utils/strings.h"

std::istream &dariadb::storage::operator>>(std::istream &in, STRATEGY &strat) {
  std::string token;
  in >> token;

  token = utils::strings::to_upper(token);

  if (token == "DYNAMIC") {
    strat = dariadb::storage::STRATEGY::DYNAMIC;
  }
  if (token == "FAST_WRITE") {
    strat = dariadb::storage::STRATEGY::FAST_WRITE;
  }
  if (token == "FAST_READ") {
    strat = dariadb::storage::STRATEGY::FAST_READ;
  }
  if (token == "COMPRESSED") {
    strat = dariadb::storage::STRATEGY::COMPRESSED;
  }
  return in;
}

std::ostream &dariadb::storage::operator<<(std::ostream &stream, const STRATEGY &strat) {
  switch (strat) {
  case STRATEGY::DYNAMIC:
    stream << "DYNAMIC";
    break;
  case STRATEGY::COMPRESSED:
    stream << "COMPRESSED";
    break;
  case STRATEGY::FAST_READ:
    stream << "FAST_READ";
    break;
  case STRATEGY::FAST_WRITE:
    stream << "FAST_WRITE";
    break;
  default:
    stream << "UNKNOW: ui16=" << (uint16_t)strat;
    break;
  };
  return stream;
}
