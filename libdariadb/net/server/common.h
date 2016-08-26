#pragma once

#include <boost/asio.hpp>

namespace dariadb {
namespace net {
typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;
}
}
