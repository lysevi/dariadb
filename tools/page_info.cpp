#include <iostream>

#include <ctime>
#include <storage/page.h>
#include <utils/fs.h>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

std::atomic_long append_count{0};
bool stop_info = false;

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  po::options_description desc("Allowed options");
  std::string storage;
  desc.add_options()("help", "produce help message")
	  ("storage", po::value<std::string>(&storage)->default_value(storage));

  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
  } catch (std::exception &ex) {
    logger("Error: " << ex.what());
    exit(1);
  }
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  try {
	  auto files = dariadb::utils::fs::ls(storage, ".page");
	  for (auto f : files) {
		  std::cout << "open " << f << std::endl;
		  auto p = dariadb::storage::Page::readHeader(f);
		  std::cout << "addeded_chunks:    " << p.addeded_chunks << std::endl;
		  std::cout << "chunk_per_storage: " << p.chunk_per_storage << std::endl;
		  std::cout << "is_closed:         " << p.is_closed << std::endl;
		  std::cout << "pos:               " << p.pos << std::endl;
	  }
  }
  catch (std::exception&ex) {
	  std::cout << "exception: " << ex.what()<<std::endl;
  }
}
