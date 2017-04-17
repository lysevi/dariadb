#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/query.h>
#include <libdariadb/scheme/ischeme.h>
#include <libdariadb/stat.h>
#include <libdariadb/status.h>
#include <libserver/net_srv_exports.h>
#include <string>

namespace dariadb {
namespace net {
namespace http {

enum class http_query_type { unknow, append, readInterval, readTimepoint, stat, scheme };

struct scheme_change_query {
  std::vector<std::string> new_params;
};

struct http_query {
  http_query_type type;
  std::shared_ptr<MeasArray> append_query;
  std::shared_ptr<QueryInterval> interval_query;
  std::shared_ptr<QueryTimePoint> timepoint_query;
  std::shared_ptr<QueryInterval> stat_query;
  std::shared_ptr<scheme_change_query> scheme_query;
};

SRV_EXPORT http_query parse_query(const dariadb::scheme::IScheme_Ptr &scheme,
                                  const std::string &query);

SRV_EXPORT std::string status2string(const dariadb::Status &s);
SRV_EXPORT std::string scheme2string(const dariadb::scheme::DescriptionMap &dm);
SRV_EXPORT std::string meases2string(const dariadb::scheme::IScheme_Ptr &scheme,
                                     const dariadb::MeasArray &ma);
SRV_EXPORT std::string stat2string(const dariadb::scheme::IScheme_Ptr &scheme,
                                   dariadb::Id id, const dariadb::Statistic &s);

using Name2IdPair = std::pair<std::string, dariadb::Id>;
SRV_EXPORT std::string
newScheme2string(const std::list<Name2IdPair> &new_names);
} // namespace http
} // namespace net
} // namespace dariadb
