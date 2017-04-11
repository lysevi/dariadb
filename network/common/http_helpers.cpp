#include <libdariadb/utils/logger.h>
#include <boost/asio.hpp>
#include <common/http_helpers.h>

using boost::asio::ip::tcp;

namespace dariadb {
namespace net {
namespace http {

http_response POST(boost::asio::io_service &service, const std::string &port,
                   const std::string &json_query) {
  http_response result;
  result.code = 0;

  tcp::resolver resolver(service);
  tcp::resolver::query query("localhost", port);
  tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

  tcp::socket socket(service);
  boost::asio::connect(socket, endpoint_iterator);

  boost::asio::streambuf request;
  std::ostream request_stream(&request);

  request_stream << "POST / HTTP/1.0\r\n";
  request_stream << "Host:"
                 << " localhost:8080"
                 << "\r\n";
  request_stream << "User-Agent: C/1.0"
                 << "\r\n";
  request_stream << "Content-Type: application/json; charset=utf-8 \r\n";
  request_stream << "Accept: */*\r\n";
  request_stream << "Content-Length: " << json_query.length() << "\r\n";
  request_stream << "Connection: close";
  request_stream << "\r\n\r\n"; // NOTE THE Double line feed
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
    logger_fatal("Invalid response");
    result.code = -1;
    return result;
  }
  result.code = status_code;
  if (status_code != 200) {
    logger_fatal("Response returned with status code ", status_code);
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

http_response GET(boost::asio::io_service &service, const std::string &port,
                  const std::string &path) {
  http_response result;
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
    logger_fatal("Invalid response");
    result.code = -1;
    return result;
  }
  result.code = status_code;
  if (status_code != 200) {
    logger_fatal("Response returned with status code ", status_code);
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

  std::stringstream ss_content;
  // Write whatever content we already have to output.
  if (response.size() > 0) {
	  ss_content << &response;
  }

  // Read until EOF, writing data to output as we go.
  boost::system::error_code error;
  while (boost::asio::read(socket, response, boost::asio::transfer_at_least(1), error)) {
	  ss_content << &response;
  }
  result.answer = ss_content.str();
  return result;
}

} // namespace http
} // namespace net
} // namespace dariadb