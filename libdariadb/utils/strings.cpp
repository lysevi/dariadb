#include "libdariadb/utils/strings.h"
#include <algorithm>
#include <clocale>
#include <ctype.h>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

std::vector<std::string> dariadb::utils::strings::tokens(const std::string &str) {
  std::vector<std::string> tokens;
  std::istringstream iss(str);
  std::copy(std::istream_iterator<std::string>(iss), std::istream_iterator<std::string>(),
            std::back_inserter(tokens));
  return tokens;
}

std::vector<std::string> dariadb::utils::strings::split(const std::string &text, char sep) {
  std::vector<std::string> tokens;
  std::size_t start = 0, end = 0;
  while ((end = text.find(sep, start)) != std::string::npos) {
    std::string temp = text.substr(start, end - start);
    if (temp != "")
      tokens.push_back(temp);
    start = end + 1;
  }
  std::string temp = text.substr(start);
  if (temp != "")
    tokens.push_back(temp);
  return tokens;
}

std::string dariadb::utils::strings::to_upper(const std::string &text) {
  std::string converted = text;

  for (size_t i = 0; i < converted.size(); ++i) {
    converted[i] = (char)toupper(converted[i]);
  }
  return converted;
}
