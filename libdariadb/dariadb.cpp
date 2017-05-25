#include <libdariadb/dariadb.h>
#include <libdariadb/engines/engine.h>
#include <libdariadb/engines/shard.h>
#include <libdariadb/utils/fs.h>
namespace dariadb {

IEngine_Ptr open_storage(const std::string &path, bool force_unlock) {
  if (utils::fs::file_exists(utils::fs::append_path(path, SHARD_FILE_NAME))) {
    return ShardEngine::create(path, force_unlock);
  } else {
    auto settings = dariadb::storage::Settings::create(path);
    IEngine_Ptr result = std::make_shared<Engine>(settings, true, force_unlock);
    return result;
  }
}

IEngine_Ptr memory_only_storage() {
  auto settings = dariadb::storage::Settings::create();
  IEngine_Ptr result = std::make_shared<Engine>(settings);
  return result;
}
} // namespace dariadb