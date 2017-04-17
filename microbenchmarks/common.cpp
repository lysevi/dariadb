#include "common.h"

namespace microbenchmark_common {
void replace_std_logger() {
  auto _raw_ptr = new BenchmarkLogger();
  auto _logger = dariadb::utils::ILogger_ptr{_raw_ptr};

  dariadb::utils::LogManager::start(_logger);
}
} // namespace microbenchmark_common