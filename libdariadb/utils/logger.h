#pragma once
#include <cstdint>
#include <iostream>
#include <string>

#define logger(msg) std::cout << "      " << msg << std::endl
#define logger_info(msg) std::cout << "[inf] " << msg << std::endl
#define logger_fatal(msg) std::cerr << "[err] " << msg << std::endl
