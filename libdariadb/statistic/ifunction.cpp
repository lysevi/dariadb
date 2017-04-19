#include <libdariadb/statistic/ifunction.h>
#include <libdariadb/utils/strings.h>

std::istream &dariadb::statistic::operator>>(std::istream &in, FUNCTION_KIND &f) {
  std::string token;
  in >> token;

  token = utils::strings::to_lower(token);
  if (token == "average") {
    f = FUNCTION_KIND::AVERAGE;
    return in;
  }
  if (token == "median") {
    f = FUNCTION_KIND::MEDIAN;
    return in;
  }
  if (token == "percentile90") {
    f = FUNCTION_KIND::PERCENTILE90;
    return in;
  }
  if (token == "percentile99") {
    f = FUNCTION_KIND::PERCENTILE99;
    return in;
  }
  return in;
}

std::ostream &dariadb::statistic::operator<<(std::ostream &stream,
                                             const FUNCTION_KIND &f) {
  switch (f) {
  case FUNCTION_KIND::AVERAGE:
    stream << "average";
    break;
  case FUNCTION_KIND::MEDIAN:
    stream << "median";
    break;
  case FUNCTION_KIND::PERCENTILE90:
    stream << "percentile90";
    break;
  case FUNCTION_KIND::PERCENTILE99:
    stream << "percentile99";
    break;
  }

  return stream;
}
