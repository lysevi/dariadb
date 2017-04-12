#include <libserver/http/connection.h>
#include <libserver/http/connection_manager.h>
#include <libserver/http/request_handler.h>
#include <boost/asio.hpp>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

using namespace dariadb::net::http;

connection::connection(boost::asio::ip::tcp::socket socket, connection_manager &manager,
                       request_handler &handler)
    : socket_(std::move(socket)), connection_manager_(manager),
      request_handler_(handler) {}

void connection::start() {
  do_headers_read();
}

void connection::stop() {
  socket_.close();
}

void connection::do_headers_read() {
  auto self(shared_from_this());

  std::string header_delim("\r\n\r\n");
  boost::asio::async_read_until(
      socket_, _request_buf, header_delim,
      [this, self](boost::system::error_code ec, std::size_t bytes_transferred) {
        if (!ec) {
          std::istream request_stream(&_request_buf);

          // parse status line "POST / HTTP/1.1\r\n"
          std::string status_line;
          std::getline(request_stream, status_line);

          size_t prev_pos = 0;
          size_t status_word_num = 0;
          for (size_t i = 0; i < status_line.size(); ++i) {
            if (status_line[i] == ' ') {
              auto begin = status_line.begin() + prev_pos;
              auto end = status_line.begin() + i;
              std::string value(begin, end);
              switch (status_word_num) {
              case size_t(0): // POST||GET
                this->request_.method = value;
                break;
              case size_t(1): // uri
                this->request_.uri = std::string(value.begin() + 1, value.end());
                break;
              }
              prev_pos = i;
              status_word_num++;
              if (status_word_num > 1) {
                // already read POST and path. protocol version unneeded to us.
                break;
              }
            }
          }

          // parse headers and find content length.
          std::string header_record;
          bool content_length_exists = false;
          bool parse_error = false;
          std::string content_length;
          while (request_stream.good() && header_record != "\r") {
            std::getline(request_stream, header_record);

            // to lower case
            std::transform(header_record.begin(), header_record.end(),
                           header_record.begin(), ::tolower);

            if (header_record != "\r") {
              auto delim_pos = std::find(header_record.begin(), header_record.end(), ':');
              if (delim_pos == header_record.end()) {
                parse_error = true;
                break;
              }
              std::string name(header_record.begin(), delim_pos);
              delim_pos++;
              delim_pos++;
              std::string value(delim_pos, header_record.end());
              header hr;
              hr.name = name;
              hr.value = value;
              this->request_.headers.push_back(hr);
              if (name == "content-length") {
                content_length_exists = true;
                content_length = value;
              }
            }
          }
          if (request_.method == "POST") {

            if (!content_length_exists) {
              reply_ = reply::stock_reply("content-length not exists.",
                                          reply::status_type::not_found);
              do_write();
              return;
            }
            if (parse_error) {
              reply_ =
                  reply::stock_reply("header parse error", reply::status_type::not_found);
              do_write();
              return;
            }
            auto query_length = std::atoll(content_length.c_str());
            do_query_read(query_length - _request_buf.size());

          } else { // GET query
            request_handler_.handle_request(request_, reply_);
            do_write();
          }
        } else if (ec != boost::asio::error::operation_aborted) {
          auto error_message = ec.message();
          logger_fatal("http: error ", error_message);
          connection_manager_.stop(shared_from_this());
        }
      });
}

void connection::do_query_read(size_t length) {
  auto self(shared_from_this());

  boost::asio::async_read(
      socket_, _request_buf, boost::asio::transfer_at_least(length),
      [this, self](boost::system::error_code ec, std::size_t bytes_transferred) {
        if (!ec) {
          std::istream request_stream(&_request_buf);

          std::string line;
          while (request_stream.good()) {
            std::getline(request_stream, line);
            request_.query += line + "\n";
          }
          request_handler_.handle_request(request_, reply_);
          do_write();
        } else if (ec != boost::asio::error::operation_aborted) {
          auto error_message = ec.message();
          logger_fatal("http: error ", error_message);
          connection_manager_.stop(shared_from_this());
        }

      });
}

void connection::do_write() {
  auto self(shared_from_this());
  boost::asio::async_write(
      socket_, reply_.to_buffers(),
      [this, self](boost::system::error_code ec, std::size_t) {
        if (!ec) {
          // Initiate graceful connection closure.
          boost::system::error_code ignored_ec;
          socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored_ec);
        }

        if (ec != boost::asio::error::operation_aborted) {
          connection_manager_.stop(shared_from_this());
        }
      });
}
