#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/scheme/ischeme.h>
#include <libdariadb/status.h>
#include <libserver/net_srv_exports.h>
#include <string>

namespace dariadb {
namespace net {
namespace http {

enum class http_query_type {
  unknow,
  append,
};

struct http_query {
  http_query_type type;
  std::shared_ptr<MeasArray> append_query;
};

SRV_EXPORT http_query parse_query(const dariadb::scheme::IScheme_Ptr &scheme,
                                  const std::string &query);

std::string status2string(const dariadb::Status &s);
std::string  scheme2string(const dariadb::scheme::DescriptionMap&dm);
}
}
}
