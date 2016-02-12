#include <ctime>
#include <iostream>
#include <cstdlib>

#include <timedb.h>

#include <boost/program_options.hpp>

namespace po = boost::program_options;


int main(int argc, char *argv[]) {
  po::options_description desc("IO benchmark.\n Allowed options");
  desc.add_options()("help", "produce help message")(
      ("verbose", "verbose ouput");

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

  if (vm.count("verbose")) {
    verbose = true;
  }

}
