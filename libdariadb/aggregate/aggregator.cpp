#include <libdariadb/aggregate/aggregator.h>

using namespace dariadb;
using namespace dariadb::aggregate;
using namespace dariadb::storage;

class Aggreagator::Private {
public:
};

Aggreagator::Aggreagator() : _Impl(std::make_unique<Aggreagator::Private>()) {}