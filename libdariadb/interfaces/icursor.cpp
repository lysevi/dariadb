#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/interfaces/icursor.h>

using namespace dariadb;

void dariadb::ICursor::apply(IReadCallback *clbk) {
  while (!this->is_end()) {
    if (clbk->is_canceled()) {
      break;
    }
    auto v = readNext();
    clbk->apply(v);
  }
}

void dariadb::ICursor::apply(IReadCallback *clbk, const dariadb::QueryInterval &q) {
  while (!this->is_end()) {
    if (clbk->is_canceled()) {
      break;
    }
    auto v = readNext();
    if (v.inQuery(q.ids, q.flag, q.from, q.to)) {
      clbk->apply(v);
    }
  }
}