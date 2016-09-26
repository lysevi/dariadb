#include "strategy.h"
#include "../utils/strings.h"

std::istream &dariadb::storage::operator>>(std::istream &in, STRATEGY &strat) {
  std::string token;
  in >> token;

  token = utils::strings::to_upper(token);

  if (token == "FAST_WRITE") {
    strat = dariadb::storage::STRATEGY::FAST_WRITE;
  }
  if (token == "COMPRESSED") {
    strat = dariadb::storage::STRATEGY::COMPRESSED;
  }
  return in;
}

std::ostream &dariadb::storage::operator<<(std::ostream &stream, const STRATEGY &strat) {
  switch (strat) {
  case STRATEGY::COMPRESSED:
    stream << "COMPRESSED";
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
