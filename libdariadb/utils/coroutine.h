#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/utils/exception.h>

#include <boost/coroutine/all.hpp>
#include <iostream>
#include <utility>
#include <vector>

namespace dariadb {
namespace utils {
namespace async {

#if BOOST_VERSION <= 105900
#define BOOST_COROUTINES_BIDIRECT
#include <boost/coroutine/all.hpp>
using Coroutine = boost::coroutines::coroutine<void()>;
using Yield = typename Coroutine::caller_type;
#else
typedef boost::coroutines::symmetric_coroutine<void> CoroT;
using Coroutine = CoroT::call_type;
using Yield = CoroT::yield_type;
#endif
}
}
}