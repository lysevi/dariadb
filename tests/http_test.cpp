#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

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

  // Try each endpoint until we successfully establish a connection.
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
  if (status_code != 200) {
    std::cout << "Response returned with status code " << status_code << "\n";
	result.code = status_code;
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

BOOST_AUTO_TEST_CASE(Post) {
  dariadb::logger("********** Connect1 **********");
  std::thread server_thread{server_thread_func};

  while (server_instance == nullptr || !server_instance->is_runned()) {
    dariadb::utils::sleep_mls(300);
  }

  auto memonly_settings = dariadb::storage::Settings::create();
  dariadb::IEngine_Ptr engine{new dariadb::Engine(memonly_settings)};
  server_instance->set_storage(engine);

  boost::asio::io_service test_service;
  auto post_result=post(test_service, std::to_string(server_param.http_port), "query");
  test_service.poll_one();

  server_instance->stop();
  server_thread.join();
}
