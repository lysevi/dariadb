#include <libdariadb/dariadb.h>
#include <libdariadb/engines/engine.h>
#include <libdariadb/engines/shard.h>
#include <libdariadb/utils/fs.h>
namespace dariadb {

IEngine_Ptr open_storage(const std::string &path) {
  if (utils::fs::file_exists(utils::fs::append_path(path, SHARD_FILE_NAME))) {
    return ShardEngine::create(path);
  } else {
    auto settings = dariadb::storage::Settings::create(path);
    IEngine_Ptr result{new Engine(settings)};
    return result;
  }
}
}