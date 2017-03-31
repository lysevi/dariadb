#include <libserver/http/query_parser.h>

#include <extern/json/src/json.hpp>

using json = nlohmann::json;

dariadb::net::http::http_query dariadb::net::http::parse_query(const std::string &query) {
  http_query result;
  result.type = http_query_type::unknow;
  json js = json::parse(query);

  std::string type = js["type"];
  if (type == "append") {
    result.type = http_query_type::append;
  }
  return result;
}