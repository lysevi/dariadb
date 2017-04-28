#include <libdariadb/engines/engine.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>

#include <boost/program_options.hpp>

#include <iostream>

namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";
bool verbose = false;
std::string set_var;
std::string new_base_name;

bool time_in_iso_format = false;
std::string erase_to;
bool stop_info = false;
bool force_unlock_storage = false;

class CtlLogger : public dariadb::utils::ILogger {
public:
  CtlLogger() {}
  ~CtlLogger() {}
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) {
    switch (kind) {
    case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
      std::cerr << msg << std::endl;
      break;
    case dariadb::utils::LOG_MESSAGE_KIND::INFO:
      if (verbose) {
        std::cout << msg << std::endl;
      }
      break;
    case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
      if (verbose) {
        std::cout << msg << std::endl;
      }
      break;
    }
  }
};

dariadb::storage::Settings_ptr loadSettings() {
  auto settings = dariadb::storage::Settings::create(storage_path);
  settings->load_min_max = false;
  return settings;
}

void check_path_exists() {
  if (!dariadb::utils::fs::path_exists(storage_path)) {
    std::cerr << "path " << storage_path << " not exists" << std::endl;
    std::exit(1);
  }
}

void show_drop_info(dariadb::Engine *storage) {
  while (!stop_info) {
    auto queue_sizes = storage->description();

    dariadb::logger_fatal(" storage: (p:", queue_sizes.pages_count,
                          " a:", queue_sizes.wal_count, " T:", queue_sizes.active_works,
                          ")", "[a:", queue_sizes.dropper.wal, "]");
    dariadb::utils::sleep_mls(2000);
  }
}

int main(int argc, char *argv[]) {
  dariadb::utils::ILogger_ptr log_ptr{new CtlLogger()};
  dariadb::utils::LogManager::start(log_ptr);

  po::options_description desc("Allowed options");
  auto aos = desc.add_options();
  aos("help", "produce help message.");
  aos("force-unlock", "force unlock storage.");
  aos("settings", "print all settings.");
  aos("format", "print storage format version.");
  aos("verbose", "verbose output.");
  aos("version", "version info.");
  aos("compress", "compress all wal files.");
  aos("iso-time", "if set, all time param is in iso format (\"20020131T235959\")");
  aos("repack", "repack all page files.");
  aos("fsck", "run force fsck.");
  aos("storage-path", po::value<std::string>(&storage_path)->default_value(storage_path),
      "path to storage.");
  aos("set", po::value<std::string>(&set_var)->default_value(set_var),
      "change setting variable.\nexample: --set=\"strategy=MEMORY\"");
  aos("create", po::value<std::string>(&new_base_name)->default_value(new_base_name),
      "create new database in selected folder.");
  aos("erase-to", po::value<std::string>(&erase_to)->default_value(erase_to),
      "erase values above a specified value. (example \"2002-01-20 "
      "23:59:59.000\").");

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (std::exception &ex) {
    dariadb::logger("Error: ", ex.what());
    exit(1);
  }
  po::notify(vm);

  if (vm.count("version")) {
    std::cout << "format: " << dariadb::Engine::format() << std::endl;
    std::cout << "version: " << dariadb::Engine::version() << std::endl;
    std::exit(0);
  }

  if (vm.count("help") || argc == 1) {
    std::cout << desc << std::endl;
    std::exit(0);
  }

  if (vm.count("verbose")) {
    dariadb::logger_info("verbose=on");
    verbose = true;
  } else {
    dariadb::logger_info("verbose=off");
  }

  if (vm.count("iso-time")) {
    dariadb::logger_info("iso-time=on");
    time_in_iso_format = true;
  } else {
    dariadb::logger_info("iso-time=off");
  }

  if (vm.count("force-unlock")) {
    dariadb::logger_info("Force unlock storage.");
    force_unlock_storage = true;
  }

  if (new_base_name.size() != 0) {
    std::cout << "create " << new_base_name << std::endl;
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto e = std::make_unique<dariadb::Engine>(settings);
    e = nullptr;
    std::exit(1);
  }

  if (vm.count("format")) {
    check_path_exists();
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto m = dariadb::storage::Manifest::create(settings);
    std::cout << "format version: " << m->get_format() << std::endl;
    std::exit(0);
  }

  if (vm.count("settings")) {
    check_path_exists();
    auto s = dariadb::storage::Settings::create(storage_path);
    std::cout << s->dump() << std::endl;
    std::exit(0);
  }

  if (set_var.size() != 0) {
    check_path_exists();
    auto s = dariadb::storage::Settings::create(storage_path);
    s->change(set_var);
    s->save();
    std::exit(0);
  }

  if (vm.count("fsck")) {
    auto settings = loadSettings();
    auto e = std::make_unique<dariadb::Engine>(settings, force_unlock_storage);
    e->fsck();
    e->stop();
  }

  if (vm.count("compress")) {
    auto settings = loadSettings();
    auto e = std::make_unique<dariadb::Engine>(settings, force_unlock_storage);
    stop_info = false;
    std::thread info_thread(&show_drop_info, e.get());
    e->compress_all();
    e->flush();
    e->stop();
    stop_info = true;
    info_thread.join();
  }

 /* if (erase_to.size() != 0) {
    dariadb::Time to = 0;
    if (time_in_iso_format) {
      to = dariadb::timeutil::from_iso_string(erase_to);
    } else {
      to = dariadb::timeutil::from_string(erase_to);
    }

    auto settings = loadSettings();
    auto e = std::make_unique<dariadb::Engine>(settings, force_unlock_storage);

    e->eraseOld(to);
    e->flush();
    e->stop();
    std::exit(0);
  }*/

  if (vm.count("repack")) {
    auto settings = loadSettings();
    auto e = std::make_unique<dariadb::Engine>(settings, force_unlock_storage);
    e->repack();
    e->flush();
    e->stop();
    std::exit(0);
  }
}
