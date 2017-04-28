#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>
#include <libserver/http/query_parser.h>
#include <extern/json/src/json.hpp>

using json = nlohmann::json;

namespace dariadb_parse_query_inner {
std::shared_ptr<dariadb::MeasArray>
read_meas_array(const dariadb::scheme::IScheme_Ptr &scheme, const json &js) {
  auto result = std::make_shared<dariadb::MeasArray>();
  auto js_iter = js.find("append_values");
  if (js_iter == js.end()) {
    dariadb::logger_fatal("append_values not found");
    return nullptr;
  }

  auto values = *js_iter;
  for (auto it = values.begin(); it != values.end(); ++it) {
    dariadb::MeasArray sub_result;
    auto id_str = it.key();
    auto val4id = it.value();

    dariadb::Id id = scheme->addParam(id_str);

    auto flag_js = val4id["F"];
    auto val_js = val4id["V"];
    auto time_js = val4id["T"];

    if (flag_js.size() != val_js.size() || time_js.size() != val_js.size()) {
      THROW_EXCEPTION("bad query format, flags:", flag_js.size(),
                      " values:", val_js.size(), " times:", time_js.size(),
                      " query: ", js.dump(1));
    }

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

    dariadb::logger("in query ", sub_result.size(), " values for id:", id);
    for (auto v : sub_result) {
      v.id = id;
      result->push_back(v);
    }
  }
  return result;
}

std::shared_ptr<dariadb::MeasArray>
read_single_meas(const dariadb::scheme::IScheme_Ptr &scheme, const json &js) {
  auto result = std::make_shared<dariadb::MeasArray>();
  result->resize(1);
  auto value = js["append_value"];
  result->front().time = value["T"];
  result->front().flag = value["F"];
  result->front().value = value["V"];
  std::string id_str = value["I"];
  result->front().id = scheme->addParam(id_str);
  return result;
}
} // namespace dariadb_parse_query_inner

dariadb::net::http::http_query
dariadb::net::http::parse_query(const dariadb::scheme::IScheme_Ptr &scheme,
                                const std::string &query) {
  http_query result;
  result.type = http_query_type::unknow;
  json js = json::parse(query);

  auto find_iter = js.find("type");
  if (find_iter == js.end()) {
    logger_fatal("query without type.");
    return result;
  }

  std::string type = *find_iter;
  if (type == "append") {
    result.type = http_query_type::append;

    auto single_value = js.find("append_value");
    if (single_value == js.end()) {
      logger("append_values query.");
      result.append_query = dariadb_parse_query_inner::read_meas_array(scheme, js);
    } else {
      logger("append_value query.");
      result.append_query = dariadb_parse_query_inner::read_single_meas(scheme, js);
    }
    if (result.append_query == nullptr) {
      logger_fatal("result.append_query is empty.");
    }
    return result;
  }

  if (type == "readInterval") {
    logger("readInterval query.");
    result.type = http_query_type::readInterval;
    dariadb::Time from = js["from"];
    dariadb::Time to = js["to"];
    dariadb::Flag flag = js["flag"];
    std::vector<std::string> values = js["id"];
    dariadb::IdArray ids;
    ids.reserve(values.size());
    auto name_map = scheme->ls();
    for (auto v : values) {
      ids.push_back(name_map.idByParam(v));
    }
    result.interval_query = std::make_shared<QueryInterval>(ids, flag, from, to);
    return result;
  }

  if (type == "readTimepoint") {
    logger("readTimepoint query.");
    result.type = http_query_type::readTimepoint;
    dariadb::Time timepoint = js["time"];
    dariadb::Flag flag = js["flag"];
    std::vector<std::string> values = js["id"];
    dariadb::IdArray ids;
    ids.reserve(values.size());
    auto name_map = scheme->ls();
    for (auto v : values) {
      ids.push_back(name_map.idByParam(v));
    }
    result.timepoint_query = std::make_shared<QueryTimePoint>(ids, flag, timepoint);
    return result;
  }

  if (type == "stat") {
    result.type = http_query_type::stat;
    dariadb::Time from = js["from"];
    dariadb::Time to = js["to"];
    std::string value = js["id"];

    dariadb::IdArray id;
    id.resize(1);
    auto name_map = scheme->ls();
    id[0] = name_map.idByParam(value);

    result.stat_query = std::make_shared<QueryInterval>(id, dariadb::Flag(), from, to);
    return result;
  }

  if (type == "scheme") {
    result.type = http_query_type::scheme;

    auto add_iter = js.find("add");
    if (add_iter != js.end()) {
      if (!add_iter->is_array()) {
        result.type = http_query_type::unknow;
        return result;
      }
      result.scheme_query = std::make_shared<scheme_change_query>();
      auto values = *add_iter;
      for (auto v : values) {
        result.scheme_query->new_params.push_back(v);
      }
    }
    return result;
  }

  if (type == "statistic") {
    logger("statistic query.");
    result.type = http_query_type::statistic;
    dariadb::Time from = js["from"];
    dariadb::Time to = js["to"];
    dariadb::Flag flag = js["flag"];
    std::string value = js["id"];
    std::vector<std::string> functions = js["functions"];

    dariadb::IdArray ids;
    ids.reserve(1);
    auto name_map = scheme->ls();
    ids.push_back(name_map.idByParam(value));

    result.statistic_calc = std::make_shared<statistic_calculation>(
        QueryInterval(ids, flag, from, to), functions);
    return result;
  }

  if (type == "erase") {
    logger("erase query.");
    result.type = http_query_type::erase;
    dariadb::Time to = js["to"];
    std::string value = js["id"];
    dariadb::IdArray ids(1);

    auto name_map = scheme->ls();
    ids[0] = name_map.idByParam(value);

    result.erase_old =
        std::make_shared<QueryInterval>(ids, dariadb::Flag(), dariadb::MIN_TIME, to);
    return result;
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

std::string dariadb::net::http::scheme2string(const dariadb::scheme::DescriptionMap &dm) {
  json js;
  for (auto kv : dm) {
    js[kv.second.name] = kv.first;
  }
  return js.dump();
}

std::string dariadb::net::http::meases2string(const dariadb::scheme::IScheme_Ptr &scheme,
                                              const dariadb::MeasArray &ma) {
  auto nameMap = scheme->ls();
  std::map<dariadb::Id, dariadb::MeasArray> values;
  for (auto &m : ma) {
    values[m.id].push_back(m);
  }

  json js_result;
  for (auto &kv : values) {
    std::list<json> js_values;
    for (auto v : kv.second) {
      json value_js;
      value_js["F"] = v.flag;
      value_js["T"] = v.time;
      value_js["V"] = v.value;
      js_values.push_back(value_js);
    }
    js_result[nameMap[kv.first].name] = js_values;
  }
  auto result = js_result.dump(1);
  return result;
}

std::string dariadb::net::http::stat2string(const dariadb::scheme::IScheme_Ptr &scheme,
                                            dariadb::Id id, const dariadb::Statistic &s) {
  json stat_js;
  stat_js["minTime"] = s.minTime;
  stat_js["maxTime"] = s.maxTime;

  stat_js["count"] = s.count;

  stat_js["minValue"] = s.minValue;
  stat_js["maxValue"] = s.maxValue;

  stat_js["sum"] = s.sum;

  auto nameMap = scheme->ls();
  auto name = nameMap[id].name;
  json result_js;
  result_js[name] = stat_js;
  return result_js.dump(1);
}

std::string
dariadb::net::http::newScheme2string(const std::list<Name2IdPair> &new_names) {
  json result;
  for (auto kv : new_names) {
    result[kv.first] = kv.second;
  }
  return result.dump(1);
}

std::string
dariadb::net::http::available_functions2string(const std::vector<std::string> &funcs) {
  json result;
  result["functions"] = funcs;
  return result.dump(1);
}

std::string dariadb::net::http::statCalculationResult2string(
    const dariadb::scheme::IScheme_Ptr &scheme, const dariadb::MeasArray &ma,
    const std::vector<std::string> &funcs) {
  json result;
  if (ma.empty()) {
    return std::string();
  }
  ENSURE(ma.size() == funcs.size());
  auto nameMap = scheme->ls();
  for (size_t i = 0; i < ma.size(); ++i) {
    json func_res;
    func_res["F"] = ma[i].flag;
    func_res["T"] = ma[i].time;
    func_res["V"] = ma[i].value;
    result[funcs[i]] = func_res;
  }

  result["measurement"] = nameMap[ma.front().id].name;
  return result.dump(1);
}
