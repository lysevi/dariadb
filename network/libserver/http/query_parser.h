#pragma once
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
};

http_query parse_query(const std::string &query);
}
}
}