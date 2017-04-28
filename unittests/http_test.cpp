#include <gtest/gtest.h>

#include "helpers.h"

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

const dariadb::net::Server::Param http_server_param(2001, 8080);

struct post_response {
  int code;
  std::string answer;
};

post_response post(boost::asio::io_service &service, std::string &port,
                   const std::string &json_query, bool set_content_length = true,
                   bool parse_error = false) {
  post_response result;
  result.code = 0;

  tcp::resolver resolver(service);
  tcp::resolver::query query("localhost", port);
  tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

  tcp::socket socket(service);
  boost::asio::connect(socket, endpoint_iterator);

  boost::asio::streambuf request;
  std::ostream request_stream(&request);
  if (!parse_error) {
    request_stream << "POST / HTTP/1.0\r\n";
    request_stream << "Host:"
                   << " localhost:8080"
                   << "\r\n";
    request_stream << "User-Agent: C/1.0"
                   << "\r\n";
    request_stream << "Content-Type: application/json; charset=utf-8 \r\n";
    request_stream << "Accept: */*\r\n";
    if (set_content_length) { // for test
      request_stream << "Content-Length: " << json_query.length() << "\r\n";
    }
    request_stream << "Connection: close";
    request_stream << "\r\n\r\n"; // NOTE THE Double line feed
    request_stream << json_query;
  } else {
    request_stream << "asddasdadad";
    request_stream << "\r\n\r\n"; // NOTE THE Double line feed
  }
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
    dariadb::logger_fatal("Invalid response");
    result.code = -1;
    return result;
  }
  result.code = status_code;
  if (status_code != 200) {
    dariadb::logger_fatal("Response returned with status code ", status_code);
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

post_response GET(boost::asio::io_service &service, std::string &port,
                  const std::string &path) {
  post_response result;
  result.code = 0;

  tcp::resolver resolver(service);
  tcp::resolver::query query("localhost", port);
  tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

  tcp::socket socket(service);
  boost::asio::connect(socket, endpoint_iterator);

  boost::asio::streambuf request;
  std::ostream request_stream(&request);

  request_stream << "GET " << path << " HTTP/1.0\r\n";
  request_stream << "Host:"
                 << " localhost:8080"
                 << "\r\n";
  request_stream << "User-Agent: C/1.0"
                 << "\r\n"
                 << "\r\n";

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
    dariadb::logger_fatal("Invalid response");
    result.code = -1;
    return result;
  }
  result.code = status_code;
  if (status_code != 200) {
    dariadb::logger_fatal("Response returned with status code ", status_code);
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

// TODO replace by fixture
static dariadb::net::Server *http_server_instance = nullptr;

void http_server_thread_func() {
  dariadb::net::Server s(http_server_param);

  EXPECT_TRUE(!s.is_runned());

  http_server_instance = &s;
  s.start();

  http_server_instance = nullptr;
}

TEST(Http, Test) {
  dariadb::logger("********** HttpTest **********");
  std::thread server_thread{http_server_thread_func};
  auto http_port = std::to_string(http_server_param.http_port);

  while (http_server_instance == nullptr || !http_server_instance->is_runned()) {
    dariadb::utils::sleep_mls(300);
  }

  auto memonly_settings = dariadb::storage::Settings::create();
  auto data_scheme = dariadb::scheme::Scheme::create(memonly_settings);
  dariadb::IEngine_Ptr engine{new dariadb::Engine(memonly_settings)};
  engine->setScheme(data_scheme);
  http_server_instance->set_storage(engine);

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
  single_value.time = dariadb::timeutil::current_time();
  single_value.value = 777;
  all_ids.insert(single_value.id);

  values[single_value.id].push_back(single_value);

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

  auto query_str = js.dump(1);
  auto post_result = post(test_service, http_port, query_str);
  EXPECT_EQ(post_result.code, 200);

  {

    json single_append_js;
    single_append_js["type"] = "append";
    json single_value_js;
    single_value_js["T"] = single_value.time;
    single_value_js["F"] = single_value.flag;
    single_value_js["V"] = single_value.value;
    single_value_js["I"] = "single_value";
    single_append_js["append_value"] = single_value_js;

    query_str = single_append_js.dump(1);
    post_result = post(test_service, http_port, query_str);
    EXPECT_EQ(post_result.code, 200);
  }

  dariadb::QueryInterval qi({all_ids.begin(), all_ids.end()}, 0, 0, dariadb::MAX_TIME);
  while (true) {
    auto all_values = engine->readInterval(qi);
    if (all_values.size() == count + 1) {
      for (auto v : all_values) {
        auto it = values.find(v.id);
        if (it == values.end()) {
          EXPECT_TRUE(false) << "id " << v.id << " not found";
        } else {
          bool founded = false;
          for (auto subv : it->second) {
            if (subv.flag == v.flag && subv.time == v.time && subv.value == v.value) {
              founded = true;
              break;
            }
          }
          EXPECT_TRUE(founded);
        }
      }
      break;
    }
  }

  dariadb::QueryInterval qi_single({single_value_id}, 0, 0, dariadb::MAX_TIME);
  auto single_interval = engine->readInterval(qi_single);
  EXPECT_EQ(single_interval.size(), size_t(1));
  EXPECT_EQ(single_interval.front().id, single_value.id);
  EXPECT_EQ(single_interval.front().time, single_value.time);
  EXPECT_EQ(single_interval.front().flag, single_value.flag);
  EXPECT_EQ(single_interval.front().value, single_value.value);

  { // add param to scheme
    json add_param_js;
    add_param_js["type"] = "scheme";
    add_param_js["add"] = {"new1", "new2", "new3"};
    query_str = add_param_js.dump(1);
    auto add_scheme_result = post(test_service, http_port, query_str);
    EXPECT_TRUE(add_scheme_result.answer.find("new1") != std::string::npos);
    EXPECT_TRUE(add_scheme_result.answer.find("new2") != std::string::npos);
    EXPECT_TRUE(add_scheme_result.answer.find("new3") != std::string::npos);
  }

  auto scheme_res = GET(test_service, http_port, "/scheme");
  EXPECT_TRUE(scheme_res.answer.find("single_value") != std::string::npos);
  EXPECT_TRUE(scheme_res.answer.find("new1") != std::string::npos);
  EXPECT_TRUE(scheme_res.answer.find("new2") != std::string::npos);
  EXPECT_TRUE(scheme_res.answer.find("new3") != std::string::npos);

  // readInterval
  {
    json readinterval_js;
    readinterval_js["type"] = "readInterval";
    readinterval_js["from"] = dariadb::MIN_TIME;
    readinterval_js["to"] = dariadb::MAX_TIME;
    readinterval_js["flag"] = dariadb::Flag();
    std::vector<std::string> values_names;
    auto values_from_scheme = engine->getScheme()->ls();
    values_names.reserve(values_from_scheme.size());
    for (auto &kv : values_from_scheme) {
      values_names.push_back(kv.second.name);
    }
    readinterval_js["id"] = values_names;

    query_str = readinterval_js.dump(1);
    post_result = post(test_service, http_port, query_str);
    EXPECT_EQ(post_result.code, 200);
    EXPECT_TRUE(post_result.answer.find("single_value") != std::string::npos);
  }

  // readTimepoint
  {
    json readinterval_js;
    readinterval_js["type"] = "readTimepoint";
    readinterval_js["time"] = dariadb::MAX_TIME;
    readinterval_js["flag"] = dariadb::Flag();

    std::vector<std::string> values_names;
    auto values_from_scheme = engine->getScheme()->ls();
    values_names.reserve(values_from_scheme.size());
    for (auto &kv : values_from_scheme) {
      values_names.push_back(kv.second.name);
    }
    readinterval_js["id"] = values_names;

    query_str = readinterval_js.dump(1);
    post_result = post(test_service, http_port, query_str);
    EXPECT_EQ(post_result.code, 200);
    EXPECT_TRUE(post_result.answer.find("single_value") != std::string::npos);
  }

  // stat
  {
    json stat_js;
    stat_js["type"] = "stat";
    stat_js["id"] = "single_value";
    stat_js["from"] = dariadb::MIN_TIME;
    stat_js["to"] = dariadb::MAX_TIME;

    query_str = stat_js.dump(1);
    post_result = post(test_service, http_port, query_str);
    EXPECT_EQ(post_result.code, 200);
    EXPECT_TRUE(post_result.answer.find("single_value") != std::string::npos);
  }

  {
     auto statistic_functions_list_res = GET(test_service, http_port, "/statfuncs");
     EXPECT_TRUE(statistic_functions_list_res.answer.find("functions") != std::string::npos);
     EXPECT_TRUE(statistic_functions_list_res.answer.find("percentile90") != std::string::npos);
  }

  {// statistic calculator
    json stat_js;
    stat_js["type"] = "statistic";
    stat_js["id"] = "single_value";
    stat_js["from"] = dariadb::MIN_TIME;
    stat_js["to"] = dariadb::MAX_TIME;
    stat_js["flag"] = dariadb::Flag();
    stat_js["functions"] = {"average","median", "percentile90"};

    query_str = stat_js.dump(1);
    post_result = post(test_service, http_port, query_str);
    EXPECT_EQ(post_result.code, 200);
    EXPECT_TRUE(post_result.answer.find("single_value") != std::string::npos);
    EXPECT_TRUE(post_result.answer.find("average") != std::string::npos);
    EXPECT_TRUE(post_result.answer.find("median") != std::string::npos);
    EXPECT_TRUE(post_result.answer.find("percentile90") != std::string::npos);
  }
  {// remove id
	  json erase_js;
	  erase_js["type"] = "erase";
	  erase_js["id"] = "single_value";
	  erase_js["to"] = dariadb::MAX_TIME;

	  query_str = erase_js.dump(1);
	  post_result = post(test_service, http_port, query_str);
	  EXPECT_EQ(post_result.code, 200);
  }
  // unknow query
  {
    json stat_js;
    stat_js["bad"] = "query";

    query_str = stat_js.dump(1);
    post_result = post(test_service, http_port, query_str);
    EXPECT_EQ(post_result.code, 404);
  }

  // parse error
  {
    post_result = post(test_service, http_port, "", false, true);
    EXPECT_EQ(post_result.code, 204); // no content
  }

  //// bad query
  //try {
  //  json stat_js;
  //  stat_js["type"] = "stat";
  //  stat_js["id"] = "single_value";
  //  stat_js["from"] = dariadb::MIN_TIME;
  //  stat_js["to"] = dariadb::MAX_TIME;

  //  query_str = stat_js.dump(1);
  //  post_result = post(test_service, http_port, query_str, false);
  //  EXPECT_EQ(post_result.code, 404);
  //} catch (...){

  //}

  http_server_instance->stop();
  server_thread.join();
}
