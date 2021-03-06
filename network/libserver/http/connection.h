#pragma once

#include <libserver/http/reply.h>
#include <libserver/http/request.h>
#include <libserver/http/request_handler.h>

#include <boost/asio.hpp>
#include <array>
#include <memory>

namespace dariadb {
namespace net {
namespace http {

class connection_manager;
const size_t reques_buffer_size = 8192;
/// Represents a single connection from a client.
class connection : public std::enable_shared_from_this<connection> {
public:
  connection(const connection &) = delete;
  connection &operator=(const connection &) = delete;

  /// Construct a connection with the given socket.
  explicit connection(boost::asio::ip::tcp::socket socket, connection_manager &manager,
                      request_handler &handler);

  /// Start the first asynchronous operation for the connection.
  void start();

  /// Stop all asynchronous operations associated with the connection.
  void stop();

private:
  void do_query_read(size_t length);
  void do_headers_read();

  /// Perform an asynchronous write operation.
  void do_write();

  /// Socket for the connection.
  boost::asio::ip::tcp::socket socket_;

  /// The manager for this connection.
  connection_manager &connection_manager_;

  /// The handler used to process the incoming request.
  request_handler &request_handler_;

  boost::asio::streambuf _request_buf;

  /// The incoming request.
  request request_;

  /// The reply to be sent back to the client.
  reply reply_;
};

typedef std::shared_ptr<connection> connection_ptr;

} // namespace http
} // namespace net
} // namespace dariadb