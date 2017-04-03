#include <libserver/http/mime_types.h>
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
  // Decode url to path.
  std::string request_path;
  if (!url_decode(req.uri, request_path)) {
    rep = reply::stock_reply("", reply::bad_request);
    return;
  }

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
  //// Request path must be absolute and not contain "..".
  // if (request_path.empty() || request_path[0] != '/' ||
  //    request_path.find("..") != std::string::npos) {
  //  rep = reply::stock_reply(reply::bad_request);
  //  return;
  //}

  //// If path ends in slash (i.e. is a directory) then add "index.html".
  // if (request_path[request_path.size() - 1] == '/') {
  //  request_path += "index.html";
  //}

  //// Determine the file extension.
  // std::size_t last_slash_pos = request_path.find_last_of("/");
  // std::size_t last_dot_pos = request_path.find_last_of(".");
  // std::string extension;
  // if (last_dot_pos != std::string::npos && last_dot_pos > last_slash_pos) {
  //  extension = request_path.substr(last_dot_pos + 1);
  //}

  //// Open the file to send back.
  // std::string full_path = doc_root_ + request_path;
  // std::ifstream is(full_path.c_str(), std::ios::in | std::ios::binary);
  // if (!is) {
  //  rep = reply::stock_reply(reply::not_found);
  //  return;
  //}

  //// Fill out the reply to be sent to the client.
  // rep.status = reply::ok;
  // char buf[512];
  // while (is.read(buf, sizeof(buf)).gcount() > 0)
  //  rep.content.append(buf, is.gcount());
  // rep.headers.resize(2);
  // rep.headers[0].name = "Content-Length";
  // rep.headers[0].value = std::to_string(rep.content.size());
  // rep.headers[1].name = "Content-Type";
  // rep.headers[1].value = mime_types::extension_to_type(extension);
}

bool request_handler::url_decode(const std::string &in, std::string &out) {
  out.clear();
  out.reserve(in.size());
  for (std::size_t i = 0; i < in.size(); ++i) {
    if (in[i] == '%') {
      if (i + 3 <= in.size()) {
        int value = 0;
        std::istringstream is(in.substr(i + 1, 2));
        if (is >> std::hex >> value) {
          out += static_cast<char>(value);
          i += 2;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else if (in[i] == '+') {
      out += ' ';
    } else {
      out += in[i];
    }
  }
  return true;
}
