#pragma once

#include <libdariadb/meas.h>
#include <string>

namespace dariadb {
namespace net {
namespace http {

enum class http_query_type {
  unknow,
  append,
};

struct http_query {
  http_query_type type;
  std::shared_ptr<MeasArray> append_query;
};

http_query parse_query(const std::string &query);
}
}
}