#include <libdariadb/interfaces/ireader.h>
#include <libdariadb/interfaces/icallbacks.h>

using namespace dariadb;
using namespace dariadb::storage;

void dariadb::IReader::apply(dariadb::storage::IReaderClb*clbk) {
  while (!this->is_end()) {
    if (clbk->is_canceled()) {
      break;
    }
    auto v = readNext();
    clbk->call(v);
  }
}
