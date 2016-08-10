#include "utils.h"
#include "exception.h"
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

std::vector<std::string> dariadb::utils::tokens(const std::string &str) {
  std::vector<std::string> tokens;
  std::istringstream iss(str);
  std::copy(std::istream_iterator<std::string>(iss), std::istream_iterator<std::string>(),
            std::back_inserter(tokens));
  return tokens;
}