#include <libdariadb/storage/readers.h>
#include <libdariadb/utils/utils.h>

using namespace dariadb;
using namespace dariadb::storage;

FullReader::FullReader(MeasArray &ml) {
  _ma = ml;
  _index = size_t(0);
}

Meas FullReader::readNext() {
  ENSURE(!is_end());

  auto result = _ma[_index++];
  return result;
}

bool FullReader::is_end() const { return _index < _ma.size(); }

Meas FullReader::top() {
  if (!is_end()) {
    return _ma[_index];
  }
  THROW_EXCEPTION("is_end()");
}
