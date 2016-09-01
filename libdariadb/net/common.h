#pragma once
#include <boost/asio.hpp>
#include <memory>

namespace dariadb {
namespace net {
typedef std::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;
typedef std::weak_ptr<boost::asio::ip::tcp::socket> socket_weak;
}
}
