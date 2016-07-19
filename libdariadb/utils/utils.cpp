#include "utils.h"
#include "exception.h"
#include <vector>
#include <string>
#include <sstream>
#include <iterator>

std::vector<std::string> dariadb::utils::tokens(const std::string&str) {
	std::vector<std::string> tokens;
	std::istringstream iss(str);
	std::copy(std::istream_iterator<std::string>(iss),
		std::istream_iterator<std::string>(), std::back_inserter(tokens));
	return tokens;
}