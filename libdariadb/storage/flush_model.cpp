#include <libdariadb/storage/flush_model.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/strings.h>
#include <sstream>

namespace dariadb {
namespace storage {

std::istream &operator>>(std::istream &in, FlushModel &fm) {
  std::string token;
  in >> token;

  token = utils::strings::to_upper(token);

  if (token == "FULL") {
    fm = FlushModel::FULL;
    return in;
  }

  if (token == "NOT_SAFETY") {
    fm = FlushModel::NOT_SAFETY;
    return in;
  }

  THROW_EXCEPTION("engine: bad flushmodel name - ", token);
}

std::ostream &operator<<(std::ostream &stream, const FlushModel &fm) {
  switch (fm) {
  case FlushModel::FULL:
    stream << "FULL";
    break;
  case FlushModel::NOT_SAFETY:
    stream << "NOT_SAFETY";
    break;
  default:
    THROW_EXCEPTION("engine: bad flushmodel - ", (uint8_t)fm);
    break;
  }
  return stream;
}

std::string to_string(const FlushModel &fm) {
  std::stringstream ss;
  ss << fm;
  return ss.str();
}
}
}