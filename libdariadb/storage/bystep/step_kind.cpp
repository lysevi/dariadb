#include <libdariadb/storage/bystep/step_kind.h>
#include <libdariadb/utils/exception.h>

std::istream &dariadb::storage::operator>>(std::istream &in, STEP_KIND &strat) {
  std::string token;
  in >> token;

  token = utils::strings::to_upper(token);

  if (token == "SECOND") {
    strat = dariadb::storage::STEP_KIND::SECOND;
    return in;
  }
  if (token == "MINUTE") {
    strat = dariadb::storage::STEP_KIND::MINUTE;
    return in;
  }

  if (token == "HOUR") {
    strat = dariadb::storage::STEP_KIND::HOUR;
    return in;
  }
  THROW_EXCEPTION("engine: bad stepKind name - ", token);
}

std::ostream &dariadb::storage::operator<<(std::ostream &stream, const STEP_KIND &strat) {
  switch (strat) {
  case STEP_KIND::SECOND:
    stream << "SECOND";
    break;
  case STEP_KIND::MINUTE:
    stream << "MINUTE";
    break;
  case STEP_KIND::HOUR:
    stream << "HOUR";
    break;
  default:
    THROW_EXCEPTION("engine: bad step kind - ", (uint16_t)strat);
    break;
  };
  return stream;
}

dariadb::Time dariadb::storage::stepByKind(const STEP_KIND stepkind) {
  dariadb::Time step = 0;
  switch (stepkind) {
  case STEP_KIND::SECOND:
    step = 1000;
    break;
  case STEP_KIND::MINUTE:
    step = 60 * 1000;
    break;
  case STEP_KIND::HOUR:
    step = 3600 * 1000;
    break;
  }
  return step;
}