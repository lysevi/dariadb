#include <libdariadb/engine.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/fs.h>

#include <boost/program_options.hpp>

#include <iostream>

namespace po = boost::program_options;

std::string storage_path = "dariadb_storage";

class CtlLogger : public dariadb::utils::ILogger {
public:
    CtlLogger(){}
    ~CtlLogger(){}
    void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg){
        switch (kind) {
        case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
            std::cerr<< msg <<std::endl;
          break;
        case dariadb::utils::LOG_MESSAGE_KIND::INFO:
            //std::cout<<msg<<std::endl;
          break;
        case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
            //std::cout<<msg<<std::endl;
          break;
        }
    }
};


int main(int argc, char *argv[]) {
    po::options_description desc("Allowed options");
    auto aos = desc.add_options();
    aos("help", "produce help message");
    aos("settings", "print all settings");
    aos("format", "print storage format version");
    aos("storage-path", po::value<std::string>(&storage_path)->default_value(storage_path), "path to storage.");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
    }
    catch (std::exception &ex) {
        dariadb::logger("Error: ", ex.what());
        exit(1);
    }
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        std::exit(0);
    }
    dariadb::utils::ILogger_ptr log_ptr{ new CtlLogger() };
    dariadb::utils::LogManager::start(log_ptr);

    if(!dariadb::utils::fs::path_exists(storage_path)){
        std::cerr<<"path not exists"<<std::endl;
        exit(1);
    }

    if (vm.count("format")) {
        dariadb::storage::Manifest m(dariadb::utils::fs::append_path(storage_path, dariadb::storage::MANIFEST_FILE_NAME));
        std::cout<<"version: "<< m.get_version()<<std::endl;
        std::exit(0);
    }

    if (vm.count("settings")) {
        dariadb::storage::Settings s(storage_path);
        std::cout<<s.dump()<<std::endl;
        std::exit(0);
    }
}
