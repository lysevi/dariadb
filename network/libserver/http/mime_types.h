#pragma once
#include <string>

namespace dariadb {
namespace net {
namespace http {
namespace mime_types {
/// Convert a file extension into a MIME type.
std::string extension_to_type(const std::string &extension);
} // namespace mime_types
} // namespace http
} // namespace net
} // namespace dariadb
