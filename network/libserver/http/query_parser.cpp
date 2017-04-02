#include <libserver/http/query_parser.h>
#include <extern/json/src/json.hpp>

using json = nlohmann::json;

namespace dariadb_parse_query_inner {
std::shared_ptr<dariadb::MeasArray> read_meas_array(const json &js) {
  auto result = std::make_shared<dariadb::MeasArray>();
  auto values = js["append_values"];
  for (auto it = values.begin(); it != values.end(); ++it) {
    dariadb::MeasArray sub_result;
    auto id_str = it.key();
    auto val4id = it.value();

    dariadb::Id id(std::atoi(id_str.c_str()));

    auto flag_js = val4id["F"];
    auto val_js = val4id["V"];
    auto time_js = val4id["T"];

    sub_result.resize(flag_js.size());
    size_t pos = 0;
    for (auto f_it = flag_js.begin(); f_it != flag_js.end(); ++f_it) {
      dariadb::Flag f = *f_it;
      sub_result[pos++].flag = f;
    }

    pos = 0;
    for (auto v_it = val_js.begin(); v_it != val_js.end(); ++v_it) {
      dariadb::Value v = *v_it;
      sub_result[pos++].value = v;
    }

    pos = 0;
    for (auto t_it = time_js.begin(); t_it != time_js.end(); ++t_it) {
      dariadb::Time t = *t_it;
      sub_result[pos++].time = t;
    }
    for (auto v : sub_result) {
      v.id = id;
      result->push_back(v);
    }
  }
  return result;
}

std::shared_ptr<dariadb::MeasArray> read_single_meas(const json &js) {
  auto result = std::make_shared<dariadb::MeasArray>();
  result->resize(1);
  auto value = js["append_value"];
  result->front().time = value["T"];
  result->front().flag = value["F"];
  result->front().value = value["V"];
  result->front().id = value["I"];
  return result;
}
} // nanespace dariadb_parse_query_inner

dariadb::net::http::http_query dariadb::net::http::parse_query(const std::string &query) {
  http_query result;
  result.type = http_query_type::unknow;
  json js = json::parse(query);

  std::string type = js["type"];
  if (type == "append") {
    result.type = http_query_type::append;

    auto single_value = js.find("append_value");
    if (single_value == js.end()) {
      result.append_query = dariadb_parse_query_inner::read_meas_array(js);
    } else {
      result.append_query = dariadb_parse_query_inner::read_single_meas(js);
    }
  }
  return result;
}

std::string dariadb::net::http::status2string(const dariadb::Status &s) {
  json js;
  js["writed"] = s.writed;
  js["ignored"] = s.ignored;
  js["error"] = to_string(s.error);
  return js.dump();
}