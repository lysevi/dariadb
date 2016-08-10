#include "utils.h"
#include "exception.h"
#include <iterator>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

std::vector<std::string> dariadb::utils::tokens(const std::string &str) {
  std::vector<std::string> tokens;
  std::istringstream iss(str);
  std::copy(std::istream_iterator<std::string>(iss), std::istream_iterator<std::string>(),
            std::back_inserter(tokens));
  return tokens;
}

std::vector<std::string> dariadb::utils::split(const std::string &text, char sep) {
    std::vector<std::string> tokens;
    std::size_t start = 0, end = 0;
    while ((end = text.find(sep, start)) != std::string::npos) {
        std::string temp = text.substr(start, end - start);
        if (temp != "") tokens.push_back(temp);
        start = end + 1;
    }
    std::string temp = text.substr(start);
    if (temp != "") tokens.push_back(temp);
    return tokens;
}
