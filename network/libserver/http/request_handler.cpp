#include <libdariadb/statistic/calculator.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/logger.h>
#include <libserver/http/query_parser.h>
#include <libserver/http/reply.h>
#include <libserver/http/request.h>
#include <libserver/http/request_handler.h>
#include <fstream>
#include <sstream>
#include <string>

using namespace dariadb::net::http;

request_handler::request_handler() : _storage_engine(nullptr) {}

namespace requers_handler_inner {
void append_query(dariadb::IEngine_Ptr storage_engine, const http_query &q, reply &rep) {
  dariadb::logger("POST query 'append': ", q.append_query->size(), " values");

  auto status = storage_engine->append(q.append_query->begin(), q.append_query->end());
  rep = reply::stock_reply(status2string(status), reply::status_type::ok);

  dariadb::logger("POST 'append' end.");
}

void interval_query(dariadb::scheme::IScheme_Ptr scheme,
                    dariadb::IEngine_Ptr storage_engine, const http_query &q,
                    reply &rep) {

  dariadb::logger("POST query 'readInterval' from:",
                  dariadb::timeutil::to_string(q.interval_query->from),
                  " to:", dariadb::timeutil::to_string(q.interval_query->to));

  auto values = storage_engine->readInterval(*q.interval_query.get());
  rep = reply::stock_reply(meases2string(scheme, values), reply::status_type::ok);
}

void timepoint_query(dariadb::scheme::IScheme_Ptr scheme,
                     dariadb::IEngine_Ptr storage_engine, const http_query &q,
                     reply &rep) {
  auto time_point_str = dariadb::timeutil::to_string(q.timepoint_query->time_point);
  dariadb::logger("POST query 'readTimepoint': ", time_point_str);

  auto values = storage_engine->readTimePoint(*q.timepoint_query.get());
  dariadb::MeasArray ma;
  ma.reserve(values.size());
  for (const auto &kv : values) {
    ma.push_back(kv.second);
  }
  rep = reply::stock_reply(meases2string(scheme, ma), reply::status_type::ok);
}

void stat_query(dariadb::scheme::IScheme_Ptr scheme, dariadb::IEngine_Ptr storage_engine,
                const http_query &q, reply &rep) {

  auto id = q.stat_query->ids[0];
  auto from = q.stat_query->from;
  auto to = q.stat_query->to;
  dariadb::logger("POST query 'stat' - id:", id,
                  " from:", dariadb::timeutil::to_string(from),
                  " to:", dariadb::timeutil::to_string(to));

  auto stat = storage_engine->stat(id, from, to);
  rep = reply::stock_reply(stat2string(scheme, id, stat), reply::status_type::ok);
}

void scheme_change_query(dariadb::scheme::IScheme_Ptr scheme, const http_query &q,
                         reply &rep) {
  std::list<std::pair<std::string, dariadb::Id>> new_names;
  for (auto v : q.scheme_query->new_params) {
    auto id = scheme->addParam(v);
    dariadb::logger_info("http: add to scheme '", v, "' = ", id);
    new_names.push_back(std::make_pair(v, id));
  }
  rep = reply::stock_reply(newScheme2string(new_names), reply::status_type::ok);
}

void statistic_calculation_query(dariadb::scheme::IScheme_Ptr scheme,
                                 dariadb::IEngine_Ptr storage_engine, const http_query &q,
                                 reply &rep) {

  auto interval = q.statistic_calc->interval;
  dariadb::logger("POST query 'statistic calculation' from:",
                  dariadb::timeutil::to_string(interval.from),
                  " to:", dariadb::timeutil::to_string(interval.to));

  dariadb::statistic::Calculator calc(storage_engine);
  auto result = calc.apply(interval.ids[0], interval.from, interval.to, interval.flag,
                           q.statistic_calc->functions);

  rep = reply::stock_reply(
      statCalculationResult2string(scheme, result, q.statistic_calc->functions),
      reply::status_type::ok);
}

void erase_query(dariadb::IEngine_Ptr storage_engine, const http_query &q, reply &rep) {
  auto interval = q.erase_old;
  dariadb::logger("POST query 'erase old' to:",
                  dariadb::timeutil::to_string(interval->to));

  storage_engine->eraseOld(interval->ids.front(), interval->to);

  rep = reply::stock_reply("", reply::status_type::ok);
}
} // namespace requers_handler_inner

void request_handler::handle_request(const request &req, reply &rep) {
  if (_storage_engine == nullptr) {
    rep = reply::stock_reply("", reply::status_type::no_content);
    return;
  }

  if (req.method == "POST") {
    dariadb::scheme::IScheme_Ptr scheme = _storage_engine->getScheme();
    if (scheme != nullptr) {
      http_query parsed_query;
      logger("http: parse query");
      try {
        parsed_query = parse_query(scheme, req.query);
      } catch (const std::exception &ex) {
        logger_fatal("http: query parse error: ", ex.what(), " query: ", req.query);
        rep = reply::stock_reply(ex.what(), reply::status_type::bad_request);
        return;
      }

      switch (parsed_query.type) {
      case http_query_type::append: {
        requers_handler_inner::append_query(_storage_engine, parsed_query, rep);
        return;
      }
      case http_query_type::readInterval: {
        requers_handler_inner::interval_query(scheme, _storage_engine, parsed_query, rep);
        return;
      }
      case http_query_type::readTimepoint: {
        requers_handler_inner::timepoint_query(scheme, _storage_engine, parsed_query,
                                               rep);
        return;
      }
      case http_query_type::stat: {
        requers_handler_inner::stat_query(scheme, _storage_engine, parsed_query, rep);
        return;
      }
      case http_query_type::scheme: {
        requers_handler_inner::scheme_change_query(scheme, parsed_query, rep);
        return;
      }
      case http_query_type::statistic: {
        requers_handler_inner::statistic_calculation_query(scheme, _storage_engine,
                                                           parsed_query, rep);
        return;
      }
      case http_query_type::erase: {
        requers_handler_inner::erase_query(_storage_engine, parsed_query, rep);
        return;
      }
      default: {
        logger_fatal("http: bad query - ", req.query);
        rep = reply::stock_reply("unknow query " + req.query,
                                 reply::status_type::not_found);
        return;
      }
      }
    } else {
      rep = reply::stock_reply("scheme does not set in engine",
                               reply::status_type::service_unavailable);
      return;
    }
  } else {
    logger("GET query ", req.uri);
    if (req.uri == "/scheme") {
      auto scheme = _storage_engine->getScheme();
      if (scheme != nullptr) {
        auto scheme_map = scheme->ls();
        auto answer = scheme2string(scheme_map);
        rep = reply::stock_reply(answer, reply::status_type::ok);
        return;
      } else {
        rep = reply::stock_reply("scheme does not set in engine",
                                 reply::status_type::service_unavailable);
        return;
      }
    }
    if (req.uri == "/statfuncs") {
      auto available_funcstions = dariadb::statistic::FunctionFactory::functions();
      auto answer = available_functions2string(available_funcstions);
      rep = reply::stock_reply(answer, reply::status_type::ok);
      return;
    }
  }
  rep = reply::stock_reply("unknow query: " + req.query, reply::status_type::no_content);
  return;
}
