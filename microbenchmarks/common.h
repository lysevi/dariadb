#pragma once
#include <libdariadb/utils/logger.h>
namespace microbenchmark_common {
class BenchmarkLogger : public dariadb::utils::ILogger {
public:
  BenchmarkLogger() {}
  ~BenchmarkLogger() {}
  void message(dariadb::utils::LOG_MESSAGE_KIND, const std::string &) {}
};

void replace_std_logger();
} // namespace microbenchmark_common