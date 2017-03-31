#include <libdariadb/utils/logger.h>
#include <libserver/http/http_server.h>
#include <signal.h>
#include <utility>

using namespace dariadb::net::http;

http_server::http_server(const std::string &address, const std::string &port,
                         boost::asio::io_service *io_service_)
    : io_service_(io_service_), signals_(*io_service_), acceptor_(*io_service_),
      connection_manager_(), socket_(*io_service_) {
  signals_.add(SIGINT);
  signals_.add(SIGTERM);
  do_await_stop();

  // Open the acceptor with the option to reuse the address (i.e. SO_REUSEADDR).
  boost::asio::ip::tcp::resolver resolver(*io_service_);
  boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve({address, port});
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen();

  do_accept();
  logger_info("http_server: started");
}

void http_server::do_accept() {
  acceptor_.async_accept(socket_, [this](boost::system::error_code ec) {
    // Check whether the server was stopped by a signal before this
    // completion handler had a chance to run.
    if (!acceptor_.is_open()) {
      return;
    }

    if (!ec) {
      connection_manager_.start(std::make_shared<connection>(
          std::move(socket_), connection_manager_, request_handler_));
    }

    do_accept();
  });
}

void http_server::do_await_stop() {
  signals_.async_wait([this](boost::system::error_code /*ec*/, int /*signo*/) {
    acceptor_.close();
    connection_manager_.stop_all();
    logger_info("http_server: do_await_stop.");
  });
}
