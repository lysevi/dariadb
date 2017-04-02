#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include <extern/json/src/json.hpp>

using json = nlohmann::json;

#include "../network/common/net_data.h"
#include <libdariadb/dariadb.h>
#include <libserver/server.h>

#include <istream>
#include <ostream>
#include <string>

#include <boost/asio.hpp>
#include <sstream>

using boost::asio::ip::tcp;

const dariadb::net::Server::Param server_param(2001);

struct post_response {
  int code;
  std::string answer;
};

post_response post(boost::asio::io_service &service, std::string &port,
                   const std::string &json_query) {
  post_response result;
  result.code = 0;

  tcp::resolver resolver(service);
  tcp::resolver::query query("localhost", port);
  tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

  tcp::socket socket(service);
  boost::asio::connect(socket, endpoint_iterator);

  boost::asio::streambuf request;
  std::ostream request_stream(&request);

  request_stream << "POST / HTTP/1.1\r\n";
  request_stream << "Host:"
                 << " localhost:8080"
                 << "\r\n";
  request_stream << "User-Agent: C/1.0"
                 << "\r\n";
  request_stream << "Content-Type: application/json; charset=utf-8 \r\n";
  request_stream << "Accept: */*\r\n";
  request_stream << "Content-Length: " << json_query.length() << "\r\n";
  request_stream << "Connection: close\r\n\r\n"; // NOTE THE Double line feed
  request_stream << json_query;

  boost::asio::write(socket, request);

  // read answer
  boost::asio::streambuf response;
  boost::asio::read_until(socket, response, "\r\n");

  // Check that response is OK.
  std::istream response_stream(&response);
  std::string http_version;
  response_stream >> http_version;
  unsigned int status_code;
  response_stream >> status_code;
  std::string status_message;
  std::getline(response_stream, status_message);
  if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
    std::cout << "Invalid response\n";
    result.code = -1;
    return result;
  }
  result.code = status_code;
  if (status_code != 200) {
    std::cout << "Response returned with status code " << status_code << "\n";
    return result;
  }

  // Read the response headers, which are terminated by a blank line.
  boost::asio::read_until(socket, response, "\r\n\r\n");

  std::stringstream ss;
  // Process the response headers.
  std::string header;
  while (std::getline(response_stream, header) && header != "\r") {
    ss << header << "\n";
  }
  ss << "\n";

  // Write whatever content we already have to output.
  if (response.size() > 0) {
    ss << &response;
  }

  // Read until EOF, writing data to output as we go.
  boost::system::error_code error;
  while (boost::asio::read(socket, response, boost::asio::transfer_at_least(1), error)) {
    ss << &response;
  }
  result.answer = ss.str();
  return result;
}

dariadb::net::Server *server_instance = nullptr;

void server_thread_func() {
  dariadb::net::Server s(server_param);

  BOOST_CHECK(!s.is_runned());

  server_instance = &s;
  s.start();

  server_instance = nullptr;
}

BOOST_AUTO_TEST_CASE(AppendTest) {
  dariadb::logger("********** Connect1 **********");
  std::thread server_thread{server_thread_func};
  auto http_port = std::to_string(server_param.http_port);

  while (server_instance == nullptr || !server_instance->is_runned()) {
    dariadb::utils::sleep_mls(300);
  }

  auto memonly_settings = dariadb::storage::Settings::create();
  auto data_scheme = dariadb::scheme::Scheme::create(memonly_settings);
  dariadb::IEngine_Ptr engine{new dariadb::Engine(memonly_settings)};
  engine->setScheme(data_scheme);
  server_instance->set_storage(engine);

  boost::asio::io_service test_service;
  json js;
  js["type"] = "append";

  dariadb::IdSet all_ids;
  std::map<dariadb::Id, dariadb::MeasArray> values;

  const size_t count = 100;
  const size_t ids = 10;

  for (size_t i = 0; i < count; ++i) {
    auto id = i % ids;
    dariadb::Meas m;
    m.id = data_scheme->addParam(std::to_string(id));
    m.time = i;
    m.flag = dariadb::Flag(i);
    m.value = dariadb::Value(i);
    values[m.id].push_back(m);
    all_ids.insert(m.id);
  }
  auto single_value_id = data_scheme->addParam("single_value");
  dariadb::Meas single_value;
  single_value.id = single_value_id;
  single_value.flag = 777;
  single_value.time = 777;
  single_value.value = 777;
  all_ids.insert(single_value.id);

  json js_query;
  for (auto &kv : values) {

    std::vector<dariadb::Flag> flags;
    std::vector<dariadb::Value> vals;
    std::vector<dariadb::Time> times;

    for (auto v : kv.second) {
      vals.push_back(v.value);
      flags.push_back(v.flag);
      times.push_back(v.time);
    }

    json ids_value;
    ids_value["F"] = flags;
    ids_value["V"] = vals;
    ids_value["T"] = times;

    js_query[std::to_string(kv.first)] = ids_value;
  }
  js["append_values"] = js_query;

  auto query_str = js.dump();
  auto post_result = post(test_service, http_port, query_str);
  BOOST_CHECK_EQUAL(post_result.code, 200);

  {

    json single_append_js;
    single_append_js["type"] = "append";
    json single_value_js;
    single_value_js["T"] = single_value.time;
    single_value_js["F"] = single_value.flag;
    single_value_js["V"] = single_value.value;
    single_value_js["I"] = "single_value";
    single_append_js["append_value"] = single_value_js;

    query_str = single_append_js.dump();
    post_result = post(test_service, http_port, query_str);
    BOOST_CHECK_EQUAL(post_result.code, 200);
  }

  dariadb::QueryInterval qi({all_ids.begin(), all_ids.end()}, 0, 0, dariadb::MAX_TIME);
  while (true) {
    auto all_values = engine->readInterval(qi);
    if (all_values.size() == count + 1) {
      for (auto v : all_values) {
        auto it = values.find(v.id);
        if (it == values.end()) {
          BOOST_TEST_MESSAGE("id " << v.id << " not found");
        } else {
          bool founded = false;
          for (auto subv : it->second) {
            if (subv.flag == v.flag && subv.time == v.time && subv.value == v.value) {
              founded = true;
              break;
            }
          }
          BOOST_CHECK(founded);
        }
      }
      break;
    }
  }

  dariadb::QueryInterval qi_single({single_value_id}, 0, 0, dariadb::MAX_TIME);
  auto single_interval = engine->readInterval(qi_single);
  BOOST_CHECK_EQUAL(single_interval.size(), size_t(1));
  BOOST_CHECK_EQUAL(single_interval.front().id, single_value.id);
  BOOST_CHECK_EQUAL(single_interval.front().time, single_value.time);
  BOOST_CHECK_EQUAL(single_interval.front().flag, single_value.flag);
  BOOST_CHECK_EQUAL(single_interval.front().value, single_value.value);
  server_instance->stop();
  server_thread.join();
}
