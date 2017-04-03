#include <libserver/http/query_parser.h>
#include <libserver/http/reply.h>
#include <libserver/http/request.h>
#include <libserver/http/request_handler.h>
#include <fstream>
#include <sstream>
#include <string>

using namespace dariadb::net::http;

request_handler::request_handler() : _storage_engine(nullptr) {}

void request_handler::handle_request(const request &req, reply &rep) {
  if (_storage_engine == nullptr) {
    rep = reply::stock_reply("", reply::no_content);
    return;
  }

  if (req.method == "POST") {
    auto scheme = _storage_engine->getScheme();
    if (scheme != nullptr) {
      auto parsed_query = parse_query(scheme, req.query);
      switch (parsed_query.type) {
      case http_query_type::append: {
        logger("POST query 'append'");
        auto status = this->_storage_engine->append(parsed_query.append_query->begin(),
                                                    parsed_query.append_query->end());
        rep = reply::stock_reply(status2string(status), reply::ok);
        return;
      }
      case http_query_type::readInterval: {
        auto values =
            this->_storage_engine->readInterval(*parsed_query.interval_query.get());
        rep = reply::stock_reply(meases2string(scheme, values), reply::ok);
        return;
      }
      case http_query_type::readTimepoint: {
        auto values =
            this->_storage_engine->readTimePoint(*parsed_query.timepoint_query.get());
        dariadb::MeasArray ma;
        ma.reserve(values.size());
        for (const auto &kv : values) {
          ma.push_back(kv.second);
        }
        rep = reply::stock_reply(meases2string(scheme, ma), reply::ok);
        return;
      }
      case http_query_type::stat: {
        auto stat = this->_storage_engine->stat(parsed_query.stat_query->ids[0],
                                                parsed_query.stat_query->from,
                                                parsed_query.stat_query->to);

        rep = reply::stock_reply(
            stat2string(scheme, parsed_query.stat_query->ids[0], stat), reply::ok);
        return;
      }
      default: {
        rep = reply::stock_reply("unknow query " + req.query, reply::no_content);
        return;
      }
      }
    } else {
      rep = reply::stock_reply("scheme does not set in engine", reply::no_content);
      return;
    }
  } else {
    logger("GET query ", req.uri);
    if (req.uri == "/scheme") {
      auto scheme = _storage_engine->getScheme();
      if (scheme != nullptr) {
        auto scheme_map = scheme->ls();
        auto answer = scheme2string(scheme_map);
        rep = reply::stock_reply(answer, reply::ok);
        return;
      } else {
        rep = reply::stock_reply("scheme does not set in engine", reply::no_content);
        return;
      }
    }
  }
  rep = reply::stock_reply("unknow query: " + req.query, reply::no_content);
  return;
}
